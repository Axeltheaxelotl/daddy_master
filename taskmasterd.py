import benedict
import asyncio
import os
import signal
import shlex
import logging
import argparse
import sys
from datetime import datetime
from enum import Enum

SOCKET_PATH = "/tmp/simple.sock"
LOG_FILE = os.path.join(os.path.dirname(__file__), "taskmaster.log")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")

def setup_logging():
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Taskmaster logging initialized")

def log_event(event: str, **details):
    msg = event
    if details:
        kv = " ".join(f"{k}={v}" for k,v in details.items())
        msg = f"{event} | {kv}"
    logging.info(msg)

class ProcessState(Enum):
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    BACKOFF = "BACKOFF"
    STOPPING = "STOPPING"
    EXITED = "EXITED"
    FATAL = "FATAL"

# Global registries
process_registry = {}            # uid -> ProcessInstance
program_index = {}               # program_name -> list[ProcessInstance]
current_program_configs = {}     # program_name -> config dict
reload_event: asyncio.Event | None = None
shutdown_in_progress = False     # Flag to prevent restarts during shutdown
recycled_uids = set()           # Available UIDs to reuse

async def parse_config(config):
    """
    Parse and validate the configuration file.
    Returns a dictionary of program configurations.
    """
    if not config or 'programs' not in config:
        raise ValueError("Invalid config: 'programs' section not found")
    
    programs = {}
    
    for program_name, program_config in config['programs'].items():
        if 'command' not in program_config:
            raise ValueError(f"Program '{program_name}' missing required field 'command'")
        
        parsed_program = {
            'name': program_name,
            'command': program_config['command'],
            'numprocs': program_config.get('numprocs', 1),
            'autostart': program_config.get('autostart', True),
            'autorestart': program_config.get('autorestart', 'unexpected'),  # always, never, unexpected
            'exitcodes': program_config.get('exitcodes', [0]),
            'starttime': program_config.get('starttime', 1),
            'startretries': program_config.get('startretries', 3),
            'stopsignal': program_config.get('stopsignal', 'TERM'),
            'stoptime': program_config.get('stoptime', 10),
            'stdout': program_config.get('stdout', 'discard'),
            'stderr': program_config.get('stderr', 'discard'),
            'env': program_config.get('env', {}),
            'workingdir': program_config.get('workingdir', '/tmp'),
            'umask': program_config.get('umask', '022'),
        }
        
        if parsed_program['autorestart'] not in ['always', 'never', 'unexpected']:
            raise ValueError(f"Invalid autorestart value for '{program_name}': {parsed_program['autorestart']}")
        if parsed_program['numprocs'] < 1:
            raise ValueError(f"numprocs must be >= 1 for '{program_name}'")
        valid_signals = ['TERM', 'INT', 'QUIT', 'KILL', 'HUP', 'USR1', 'USR2']
        if parsed_program['stopsignal'] not in valid_signals:
            raise ValueError(f"Invalid stopsignal for '{program_name}': {parsed_program['stopsignal']}")
        
        try:
            parsed_program['umask'] = int(parsed_program['umask'], 8)
        except ValueError:
            raise ValueError(f"Invalid umask for '{program_name}': {parsed_program['umask']}")
        
        programs[program_name] = parsed_program
    
    return programs

class ProcessInstance:
    """Represents a single process instance with unique UID"""
    _uid_counter = 0
    
    def __init__(self, program_config, instance_num):
        global recycled_uids
        
        # Reuse recycled UID if available, otherwise increment counter
        if recycled_uids:
            self.uid = recycled_uids.pop()
        else:
            ProcessInstance._uid_counter += 1
            self.uid = ProcessInstance._uid_counter
        self.program_name = program_config['name']
        self.instance_num = instance_num
        self.config = program_config
        
        # Process state
        self.state = ProcessState.STOPPED
        self.process = None
        self.pid = None
        self.retries = 0
        self.start_time = None
        self.stop_time = None
        
        # File handles for logging
        self.stdout_file = None
        self.stderr_file = None
        
    def get_display_name(self):
        """Get display name like 'sleeper:0' or 'sleeper:1'"""
        if self.config['numprocs'] > 1:
            return f"{self.program_name}:{self.instance_num}"
        return self.program_name
    
    async def start(self, manual=False):
        """Start the process"""
        if self.state in [ProcessState.RUNNING, ProcessState.STARTING]:
            print(f"[{self.uid}] {self.get_display_name()} is already running")
            return False
        
        # Reset retries on manual start or if state was FATAL
        if manual or self.state == ProcessState.FATAL:
            self.retries = 0
            if self.state == ProcessState.FATAL:
                print(f"[{self.uid}] {self.get_display_name()} resetting from FATAL state")
        
        if self.retries >= self.config['startretries']:
            self.state = ProcessState.FATAL
            print(f"[{self.uid}] {self.get_display_name()} exceeded max retries, marked as FATAL")
            return False
        
        try:
            self.state = ProcessState.STARTING
            self.retries += 1
            print(f"[{self.uid}] Starting {self.get_display_name()} (attempt {self.retries}/{self.config['startretries']})")
            
            # Parse command
            cmd_parts = shlex.split(self.config['command'])
            
            # Prepare environment
            env = os.environ.copy()
            env.update(self.config['env'])
            
            # Setup stdout/stderr
            stdout_target = await self._get_log_file('stdout')
            stderr_target = await self._get_log_file('stderr')
            
            # Start process
            self.process = await asyncio.create_subprocess_exec(
                *cmd_parts,
                stdout=stdout_target,
                stderr=stderr_target,
                env=env,
                cwd=self.config['workingdir'],
                preexec_fn=lambda: os.umask(self.config['umask'])
            )
            
            self.pid = self.process.pid
            self.start_time = datetime.now()
            print(f"[{self.uid}] {self.get_display_name()} started with PID {self.pid}")
            
            # Wait for starttime to confirm it's running
            await asyncio.sleep(self.config['starttime'])
            
            # Check if process is still alive
            if self.process.returncode is None:
                self.state = ProcessState.RUNNING
                self.retries = 0  # Reset retries on successful start
                print(f"[{self.uid}] {self.get_display_name()} is RUNNING")
                
                # Monitor the process
                asyncio.create_task(self._monitor_process())
                return True
            else:
                self.state = ProcessState.BACKOFF
                print(f"[{self.uid}] {self.get_display_name()} exited during startup")
                await self._handle_unexpected_exit(self.process.returncode)
                return False
                
        except Exception as e:
            self.state = ProcessState.BACKOFF
            print(f"[{self.uid}] Error starting {self.get_display_name()}: {e}")
            await self._handle_unexpected_exit(None)
            return False
    
    async def _get_log_file(self, log_type):
        """Get stdout or stderr file handle"""
        log_path = self.config[log_type]
        
        if log_path == 'discard':
            return asyncio.subprocess.DEVNULL
        
        try:
            # For multiple processes, append instance number to filename
            if self.config['numprocs'] > 1:
                # Transform /tmp/app.log -> /tmp/app_0.log, /tmp/app_1.log etc
                path_parts = log_path.rsplit('.', 1)
                if len(path_parts) == 2:
                    log_path = f"{path_parts[0]}_{self.instance_num}.{path_parts[1]}"
                else:
                    log_path = f"{log_path}_{self.instance_num}"
            
            # Add restart separator with timestamp on manual restart
            if log_type == 'stdout':
                self.stdout_file = open(log_path, 'a')
                # Add separator on restart
                if self.retries > 1 or (hasattr(self, '_was_restarted') and self._was_restarted):
                    separator = f"\n=== RESTARTED {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (attempt {self.retries}) ===\n"
                    self.stdout_file.write(separator)
                    self.stdout_file.flush()
                return self.stdout_file
            else:
                self.stderr_file = open(log_path, 'a')
                # Add separator on restart
                if self.retries > 1 or (hasattr(self, '_was_restarted') and self._was_restarted):
                    separator = f"\n=== RESTARTED {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (attempt {self.retries}) ===\n"
                    self.stderr_file.write(separator)
                    self.stderr_file.flush()
                return self.stderr_file
        except Exception as e:
            print(f"[{self.uid}] Error opening {log_type} file {log_path}: {e}")
            return asyncio.subprocess.DEVNULL
    
    async def _monitor_process(self):
        """Monitor process and handle exits"""
        if not self.process:
            return
        
        returncode = await self.process.wait()
        self.stop_time = datetime.now()
        self.state = ProcessState.EXITED
        
        print(f"[{self.uid}] {self.get_display_name()} exited with code {returncode}")
        
        # Close log files
        self._close_log_files()
        
        # Handle restart policy
        await self._handle_unexpected_exit(returncode)
    
    async def _handle_unexpected_exit(self, returncode):
        """Handle process exit based on autorestart policy"""
        global shutdown_in_progress
        
        # Don't restart if shutdown is in progress
        if shutdown_in_progress:
            print(f"[{self.uid}] {self.get_display_name()} exited during shutdown, not restarting")
            return
        
        # Don't restart if manually stopped
        if hasattr(self, '_manual_stop') and self._manual_stop:
            print(f"[{self.uid}] {self.get_display_name()} was manually stopped, not restarting")
            self._manual_stop = False  # Reset flag
            return
        
        autorestart = self.config['autorestart']
        expected_exit = returncode in self.config['exitcodes']
        
        should_restart = False
        
        if autorestart == 'always':
            should_restart = True
        elif autorestart == 'unexpected' and not expected_exit:
            should_restart = True
        elif autorestart == 'never':
            should_restart = False
        
        if should_restart and self.retries < self.config['startretries']:
            print(f"[{self.uid}] {self.get_display_name()} will restart (autorestart={autorestart})")
            await asyncio.sleep(1)  # Brief delay before restart
            await self.start()
        else:
            if self.retries >= self.config['startretries']:
                self.state = ProcessState.FATAL
            print(f"[{self.uid}] {self.get_display_name()} will not restart (state={self.state.value})")
    
    async def stop(self, manual=False):
        """Stop the process"""
        if self.state not in [ProcessState.RUNNING, ProcessState.STARTING]:
            print(f"[{self.uid}] {self.get_display_name()} is not running (state={self.state.value})")
            return False
        
        if not self.process:
            self.state = ProcessState.STOPPED
            return True
        
        try:
            self.state = ProcessState.STOPPING
            # Mark as manually stopped to prevent restart
            if manual:
                self._manual_stop = True
            print(f"[{self.uid}] Stopping {self.get_display_name()} (PID {self.pid})")
            
            # Send configured stop signal
            sig = getattr(signal, f'SIG{self.config["stopsignal"]}')
            self.process.send_signal(sig)
            
            # Wait for graceful shutdown
            try:
                await asyncio.wait_for(self.process.wait(), timeout=self.config['stoptime'])
                print(f"[{self.uid}] {self.get_display_name()} stopped gracefully")
            except asyncio.TimeoutError:
                print(f"[{self.uid}] {self.get_display_name()} did not stop in time, sending SIGKILL")
                self.process.kill()
                await self.process.wait()
            
            self.state = ProcessState.STOPPED
            self.pid = None
            self.stop_time = datetime.now()
            self._close_log_files()
            return True
            
        except Exception as e:
            print(f"[{self.uid}] Error stopping {self.get_display_name()}: {e}")
            return False
    
    def _close_log_files(self):
        """Close log file handles"""
        if self.stdout_file:
            self.stdout_file.close()
            self.stdout_file = None
        if self.stderr_file:
            self.stderr_file.close()
            self.stderr_file = None
    
    def get_status(self):
        """Get status information"""
        uptime = None
        if self.start_time and self.state == ProcessState.RUNNING:
            uptime = (datetime.now() - self.start_time).total_seconds()
        
        return {
            'uid': self.uid,
            'name': self.get_display_name(),
            'state': self.state.value,
            'pid': self.pid,
            'uptime': uptime,
            'retries': self.retries
        }

async def start_programs(programs):
    """Instantiate and optionally start processes for all program configs."""
    global process_registry, program_index, current_program_configs
    for program_name, program_config in programs.items():
        current_program_configs[program_name] = program_config
        program_index.setdefault(program_name, [])
        numprocs = program_config['numprocs']
        for i in range(numprocs):
            proc = ProcessInstance(program_config, i)
            process_registry[proc.uid] = proc
            program_index[program_name].append(proc)
            if program_config['autostart']:
                await proc.start()
                log_event("PROCESS_STARTED", program=proc.get_display_name(), uid=proc.uid, pid=proc.pid)
            else:
                log_event("PROCESS_REGISTERED", program=proc.get_display_name(), uid=proc.uid, autostart=False)
    log_event("REGISTRY_INITIALIZED", total_processes=len(process_registry))

def format_uptime(seconds):
    """Format uptime in human readable form"""
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds//60)}m {int(seconds%60)}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

def format_status_table() -> str:
    """Format process status as a nice table"""
    if not process_registry:
        return "No processes configured"
    
    # Calculate column widths
    rows = []
    for proc in process_registry.values():
        st = proc.get_status()
        rows.append({
            'uid': str(st['uid']),
            'name': st['name'],
            'state': st['state'],
            'pid': str(st['pid']) if st['pid'] else '-',
            'uptime': format_uptime(st['uptime']),
            'retries': str(st['retries'])
        })
    
    # Calculate max widths
    headers = {'uid': 'UID', 'name': 'NAME', 'state': 'STATE', 'pid': 'PID', 'uptime': 'UPTIME', 'retries': 'RETRIES'}
    widths = {}
    for col in headers:
        widths[col] = max(len(headers[col]), max(len(row[col]) for row in rows) if rows else 0)
    
    lines = []
    header = " | ".join(headers[col].ljust(widths[col]) for col in ['uid', 'name', 'state', 'pid', 'uptime', 'retries'])
    lines.append(header)
    lines.append("-" * len(header))
    
    for row in rows:
        line = " | ".join(row[col].ljust(widths[col]) for col in ['uid', 'name', 'state', 'pid', 'uptime', 'retries'])
        lines.append(line)
    
    return "\n".join(lines)

# Global attachment sessions
attachment_sessions = {}  # client_id -> {'program': str, 'task': asyncio.Task}

async def start_command(target: str) -> str:
    if target == 'all':
        msgs = []
        for p in process_registry.values():
            result = await p.start(manual=True)
            if result:
                msgs.append(f"{p.get_display_name()} started")
            else:
                msgs.append(f"{p.get_display_name()} failed to start")
        return "; ".join(msgs) if msgs else "No processes to start"
    if target in program_index:
        msgs = []
        for p in program_index[target]:
            result = await p.start(manual=True)
            if result:
                msgs.append(f"{p.get_display_name()} started")
            else:
                msgs.append(f"{p.get_display_name()} failed to start")
        return "; ".join(msgs)
    return f"Unknown program '{target}'"

async def stop_command(target: str) -> str:
    if target == 'all':
        msgs = []
        for p in process_registry.values():
            result = await p.stop(manual=True)
            if result:
                msgs.append(f"{p.get_display_name()} stopped")
            else:
                msgs.append(f"{p.get_display_name()} was not running")
        return "; ".join(msgs) if msgs else "No processes to stop"
    if target in program_index:
        msgs = []
        for p in program_index[target]:
            result = await p.stop(manual=True)
            if result:
                msgs.append(f"{p.get_display_name()} stopped")
            else:
                msgs.append(f"{p.get_display_name()} was not running")
        return "; ".join(msgs)
    return f"Unknown program '{target}'"

async def restart_command(target: str) -> str:
    if target == 'all':
        msgs = []
        for p in process_registry.values():
            await p.stop(manual=True)  # Manual stop to prevent autorestart
            result = await p.start(manual=True)
            if result:
                msgs.append(f"{p.get_display_name()} restarted")
            else:
                msgs.append(f"{p.get_display_name()} failed to restart")
        return "; ".join(msgs) if msgs else "No processes to restart"
    if target in program_index:
        msgs = []
        for p in program_index[target]:
            await p.stop(manual=True)  # Manual stop to prevent autorestart
            result = await p.start(manual=True)
            if result:
                msgs.append(f"{p.get_display_name()} restarted")
            else:
                msgs.append(f"{p.get_display_name()} failed to restart")
        return "; ".join(msgs)
    return f"Unknown program '{target}'"

async def shutdown_all_processes():
    """Stop all managed processes gracefully during shutdown"""
    global process_registry, shutdown_in_progress
    
    # Set shutdown flag to prevent restarts
    shutdown_in_progress = True
    
    if not process_registry:
        log_event("SHUTDOWN_NO_PROCESSES")
        return
    
    log_event("SHUTDOWN_STARTING", total_processes=len(process_registry))
    print(f"Shutting down {len(process_registry)} processes...")
    
    # Stop all processes
    stop_tasks = []
    for proc in process_registry.values():
        if proc.state in [ProcessState.RUNNING, ProcessState.STARTING]:
            stop_tasks.append(proc.stop())
    
    if stop_tasks:
        await asyncio.gather(*stop_tasks, return_exceptions=True)
    
    log_event("SHUTDOWN_COMPLETE", stopped_processes=len(stop_tasks))
    print("All processes stopped.")

async def reload_configuration() -> str:
    """Reload config on demand or SIGHUP."""
    cfg_raw = config_loader()
    if cfg_raw is None:
        return "Reload failed: could not load config"
    try:
        new_programs = await parse_config(cfg_raw)
    except Exception as e:
        return f"Reload failed: {e}"
    await apply_config_diff(new_programs)
    log_event("CONFIG_RELOADED", programs=len(new_programs))
    return "Configuration reloaded"

async def apply_config_diff(new_programs: dict):
    """Apply differences between current and new program configurations."""
    global current_program_configs, program_index, process_registry
    old_names = set(current_program_configs.keys())
    new_names = set(new_programs.keys())
    removed = old_names - new_names
    added = new_names - old_names
    possibly_changed = old_names & new_names

    # Remove programs
    for name in removed:
        for proc in program_index.get(name, []):
            await proc.stop()
            log_event("PROCESS_REMOVED", program=proc.get_display_name(), uid=proc.uid)
            recycled_uids.add(proc.uid)  # Recycle the UID
            process_registry.pop(proc.uid, None)
        program_index.pop(name, None)
        current_program_configs.pop(name, None)

    # Add new programs
    for name in added:
        cfg = new_programs[name]
        current_program_configs[name] = cfg
        program_index.setdefault(name, [])
        for i in range(cfg['numprocs']):
            proc = ProcessInstance(cfg, i)
            process_registry[proc.uid] = proc
            program_index[name].append(proc)
            if cfg['autostart']:
                await proc.start()
                log_event("PROCESS_STARTED", program=proc.get_display_name(), uid=proc.uid, pid=proc.pid)
            else:
                log_event("PROCESS_REGISTERED", program=proc.get_display_name(), uid=proc.uid, autostart=False)

    # Changed programs (restart all instances if any field differs)
    for name in possibly_changed:
        old_cfg = current_program_configs[name]
        new_cfg = new_programs[name]
        if old_cfg != new_cfg:
            log_event("PROGRAM_CHANGED", program=name)
            # stop & remove old instances
            for proc in program_index.get(name, []):
                await proc.stop()
                recycled_uids.add(proc.uid)  # Recycle the UID
                process_registry.pop(proc.uid, None)
            program_index[name] = []
            current_program_configs[name] = new_cfg
            # create new instances
            for i in range(new_cfg['numprocs']):
                proc = ProcessInstance(new_cfg, i)
                process_registry[proc.uid] = proc
                program_index[name].append(proc)
                if new_cfg['autostart']:
                    await proc.start()
                    log_event("PROCESS_STARTED", program=proc.get_display_name(), uid=proc.uid, pid=proc.pid)
                else:
                    log_event("PROCESS_REGISTERED", program=proc.get_display_name(), uid=proc.uid, autostart=False)

def get_log_path_for_instance(proc):
    """Get the actual log path for a process instance"""
    log_path = proc.config['stdout']
    if log_path == 'discard':
        return None
    
    # For multiple processes, append instance number
    if proc.config['numprocs'] > 1:
        path_parts = log_path.rsplit('.', 1)
        if len(path_parts) == 2:
            log_path = f"{path_parts[0]}_{proc.instance_num}.{path_parts[1]}"
        else:
            log_path = f"{log_path}_{proc.instance_num}"
    
    return log_path

def tail_program_output(name: str, lines: int = 10) -> str:
    """Return last lines of stdout log for the first instance of a program."""
    if name not in program_index or not program_index[name]:
        return f"Unknown program '{name}'"
    proc = program_index[name][0]
    log_path = get_log_path_for_instance(proc)
    if not log_path:
        return "stdout discarded"
    try:
        with open(log_path, 'r') as f:
            content = f.readlines()
        return ''.join(content[-lines:]) if content else "(empty)"
    except Exception as e:
        return f"Cannot read log: {e}"

def clear_logs_command(target: str) -> str:
    """Clear log files for a program"""
    if target == 'all':
        cleared = []
        for proc in process_registry.values():
            log_path = get_log_path_for_instance(proc)
            if log_path and log_path != 'discard':
                try:
                    with open(log_path, 'w') as f:
                        f.write(f"=== LOG CLEARED {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                    cleared.append(proc.get_display_name())
                except Exception as e:
                    cleared.append(f"{proc.get_display_name()} (error: {e})")
        return f"Cleared logs for: {', '.join(cleared)}" if cleared else "No logs to clear"
    
    if target in program_index:
        cleared = []
        for proc in program_index[target]:
            log_path = get_log_path_for_instance(proc)
            if log_path and log_path != 'discard':
                try:
                    with open(log_path, 'w') as f:
                        f.write(f"=== LOG CLEARED {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                    cleared.append(proc.get_display_name())
                except Exception as e:
                    cleared.append(f"{proc.get_display_name()} (error: {e})")
        return f"Cleared logs for: {', '.join(cleared)}" if cleared else "No logs to clear"
    
    return f"Unknown program '{target}'"

async def attach_to_program(client_writer, name: str, instance_num: int = 0) -> str:
    """Attach to a program's stdout and stream it in real-time"""
    if name not in program_index or not program_index[name]:
        return f"Unknown program '{name}'"
    
    # Select specific instance or default to first
    if instance_num >= len(program_index[name]):
        return f"Program '{name}' only has {len(program_index[name])} instances (0-{len(program_index[name])-1})"
    
    proc = program_index[name][instance_num]
    if proc.config['stdout'] == 'discard':
        return "Program stdout is discarded, cannot attach"
    
    if proc.state != ProcessState.RUNNING:
        return f"Program {proc.get_display_name()} is not running (state: {proc.state.value})"
    
    log_path = get_log_path_for_instance(proc)
    if not log_path:
        return "Program stdout is discarded, cannot attach"
    
    client_id = id(client_writer)
    
    try:
        # Send initial content
        with open(log_path, 'r') as f:
            existing = f.read()
        if existing:
            client_writer.write(f"=== Attached to {proc.get_display_name()} (showing recent output) ===\n".encode())
            client_writer.write(existing[-2000:].encode())  # Last 2KB
            await client_writer.drain()
        
        client_writer.write(f"=== Live output (Press Ctrl+C to detach) ===\n".encode())
        await client_writer.drain()
        
        # Start tailing
        async def tail_file():
            try:
                # Get current file position
                with open(log_path, 'r') as f:
                    f.seek(0, 2)  # Seek to end
                    pos = f.tell()
                
                while True:
                    with open(log_path, 'r') as f:
                        f.seek(pos)
                        new_data = f.read()
                        if new_data:
                            client_writer.write(new_data.encode())
                            await client_writer.drain()
                            pos = f.tell()
                    await asyncio.sleep(0.1)  # Poll every 100ms
            except Exception as e:
                try:
                    client_writer.write(f"\n=== Attachment error: {e} ===\n".encode())
                    await client_writer.drain()
                except:
                    pass
        
        # Store the task
        task = asyncio.create_task(tail_file())
        attachment_sessions[client_id] = {'program': proc.get_display_name(), 'task': task}
        
        return None  # Signal that we're in attachment mode
        
    except Exception as e:
        return f"Cannot attach to {proc.get_display_name()}: {e}"

def get_help_text() -> str:
    """Get comprehensive help text"""
    return """Taskmaster Control Commands:

Process Management:
  status                    Show status of all programs in table format
  start <program>|all       Start a program or all programs
  stop <program>|all        Stop a program or all programs  
  restart <program>|all     Restart a program or all programs

Information:
  tail <program> [lines]    Show last N lines of program output (default: 10)
  attach <program[:instance]> Attach to program's live output stream (Ctrl+C to detach)
  clear-logs <program>|all  Clear log files for program(s)

Configuration:
  reload                    Reload configuration file (same as SIGHUP)

System:
  clear                     Clear the terminal screen (client-side command)
  help                      Show this help message
  exit, quit                Disconnect from daemon (daemon keeps running)
  shutdown                  Shutdown the taskmaster daemon completely

Notes:
- Program names are defined in your config.yaml file
- Use 'all' to operate on all configured programs
- For programs with numprocs > 1, logs are automatically separated (e.g., app_0.log, app_1.log)
- Use program:instance syntax to target specific instances (e.g., sleeper:0, sleeper:1)
- Attachment mode streams output live until you press Ctrl+C
- Config reload preserves running processes that haven't changed
- Client automatically reconnects if connection is lost
"""

async def handle_client(reader, writer):
    client_id = id(writer)
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            request = data.decode().strip()
            parts = request.split()
            if not parts:
                continue
            cmd = parts[0].lower()
            args = parts[1:]
            log_event("CLIENT_COMMAND", cmd=cmd, args=' '.join(args))
            
            # Handle commands
            if cmd == 'status':
                resp = format_status_table()
            elif cmd == 'start' and args:
                resp = await start_command(args[0])
            elif cmd == 'stop' and args:
                resp = await stop_command(args[0])
            elif cmd == 'restart' and args:
                resp = await restart_command(args[0])
            elif cmd == 'reload':
                resp = await reload_configuration()
            elif cmd == 'tail' and args:
                lines = int(args[1]) if len(args) > 1 and args[1].isdigit() else 10
                resp = tail_program_output(args[0], lines)
            elif cmd == 'attach' and args:
                # Parse attach command: "attach program" or "attach program:instance"
                target = args[0]
                instance_num = 0
                if ':' in target:
                    program_name, instance_str = target.split(':', 1)
                    try:
                        instance_num = int(instance_str)
                    except ValueError:
                        resp = f"Invalid instance number: {instance_str}"
                        writer.write(resp.encode())
                        await writer.drain()
                        continue
                else:
                    program_name = target
                
                resp = await attach_to_program(writer, program_name, instance_num)
                if resp is None:  # Attachment mode started
                    # Wait for client to disconnect or send interrupt
                    try:
                        while True:
                            data = await reader.read(1024)
                            if not data:
                                break
                            # Client sent data (likely Ctrl+C), detach
                            break
                    except:
                        pass
                    finally:
                        # Clean up attachment
                        if client_id in attachment_sessions:
                            attachment_sessions[client_id]['task'].cancel()
                            del attachment_sessions[client_id]
                        writer.write(b"\n=== Detached ===\n")
                        await writer.drain()
                    continue  # Skip normal response handling
            elif cmd == 'clear-logs' and args:
                resp = clear_logs_command(args[0])
            elif cmd == 'help':
                resp = get_help_text()
            elif cmd in ['exit', 'quit', 'shutdown']:
                if cmd == 'shutdown':
                    resp = "Shutting down taskmaster daemon..."
                    writer.write(resp.encode())
                    await writer.drain()
                    
                    # Stop all processes before shutting down
                    await shutdown_all_processes()
                    raise asyncio.CancelledError()
                else:
                    # Just exit client commands
                    resp = "Client disconnecting..."
                    writer.write(resp.encode())
                    await writer.drain()
                    return  # Exit this client handler without shutting down daemon
            else:
                resp = f"Unknown command '{cmd}'. Type 'help' for available commands."
            
            writer.write((resp + '\n').encode())
            await writer.drain()
    except asyncio.CancelledError:
        log_event("SERVER_TERMINATION_REQUEST")
    except Exception as e:
        log_event("CLIENT_ERROR", error=str(e))
    finally:
        # Clean up any attachment session
        if client_id in attachment_sessions:
            attachment_sessions[client_id]['task'].cancel()
            del attachment_sessions[client_id]
        writer.close()
        await writer.wait_closed()
        log_event("CLIENT_DISCONNECTED")

async def server_loop(config):
    if os.path.exists(SOCKET_PATH):
        os.unlink(SOCKET_PATH)
    
    server = await asyncio.start_unix_server(
        handle_client,
        path=SOCKET_PATH
    )
    
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')
    
    async with server:
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            print("Server shutting down...")
        finally:
            if os.path.exists(SOCKET_PATH):
                os.unlink(SOCKET_PATH)

def config_loader(config_path=None):
    global CONFIG_PATH
    if config_path:
        CONFIG_PATH = config_path
    
    try:
        config = benedict.load_yaml_file(CONFIG_PATH)
        return config
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return None

def daemonize():
    """Fork process to run in background"""
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process, exit
            sys.exit(0)
    except OSError as e:
        print(f"Fork failed: {e}")
        sys.exit(1)
    
    # Become session leader
    os.setsid()
    
    # Fork again to prevent zombie
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        print(f"Second fork failed: {e}")
        sys.exit(1)
    
    # Redirect standard streams
    os.chdir("/")
    os.umask(0)
    
    # Close all file descriptors
    import resource
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if maxfd == resource.RLIM_INFINITY:
        maxfd = 1024
    
    for fd in range(maxfd):
        try:
            os.close(fd)
        except OSError:
            pass
    
    # Redirect stdin, stdout, stderr to /dev/null
    os.open("/dev/null", os.O_RDWR)  # stdin
    os.dup2(0, 1)  # stdout
    os.dup2(0, 2)  # stderr

async def main():
    parser = argparse.ArgumentParser(description="Taskmaster Process Control Daemon")
    parser.add_argument('-d', '--daemon', action='store_true', 
                       help='Run as daemon in background')
    parser.add_argument('-c', '--config', default='config.yaml',
                       help='Configuration file path (default: config.yaml)')
    args = parser.parse_args()
    
    if args.daemon:
        print("Starting taskmaster daemon...")
        daemonize()
    
    # detect root privileges
    if os.geteuid() == 0:
        # deescend privileges to user 'nobody'
        import pwd
        nobody = pwd.getpwnam('nobody')
        os.setgid(nobody.pw_gid)
        os.setuid(nobody.pw_uid)

    setup_logging()
    log_event("TASKMASTER_STARTING", daemon_mode=args.daemon)
    
    config = config_loader(args.config)
    if config is None:
        if not args.daemon:
            print("Failed to load configuration. Exiting.")
        log_event("CONFIG_LOAD_FAILED")
        return
    programs = await parse_config(config)
    await start_programs(programs)
    
    # Setup reload signal
    global reload_event
    reload_event = asyncio.Event()
    def _hup_handler(signum, frame):
        if reload_event:
            reload_event.set()
            log_event("SIGHUP_RECEIVED")
    signal.signal(signal.SIGHUP, _hup_handler)
    
    async def _reload_watcher():
        while True:
            await reload_event.wait()
            reload_event.clear()
            await reload_configuration()
    asyncio.create_task(_reload_watcher())
    
    # Setup shutdown signal handlers (only for daemon mode)
    if args.daemon:
        shutdown_event = asyncio.Event()
        def _shutdown_handler(signum, frame):
            log_event("SHUTDOWN_SIGNAL_RECEIVED", signal=signum)
            shutdown_event.set()
        
        signal.signal(signal.SIGTERM, _shutdown_handler)
        signal.signal(signal.SIGINT, _shutdown_handler)
        
        async def _shutdown_watcher():
            await shutdown_event.wait()
            print("Received shutdown signal, stopping all processes...")
            await shutdown_all_processes()
            # Cancel the server
            raise asyncio.CancelledError()
        
        shutdown_task = asyncio.create_task(_shutdown_watcher())
    
    if not args.daemon:
        print("Taskmaster started in foreground mode. Press Ctrl+C to stop.")
        print(f"Control socket: {SOCKET_PATH}")
        print("Use taskmasterctl.py to control processes.")
    
    try:
        # In daemon mode, run server alongside shutdown watcher
        if args.daemon:
            await asyncio.gather(server_loop(programs), shutdown_task)
        else:
            await server_loop(programs)
    except asyncio.CancelledError:
        # Shutdown was requested
        if not args.daemon:
            print("Taskmaster server stopped.")
        log_event("TASKMASTER_SERVER_STOPPED")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nReceived interrupt signal, shutting down...")
        # Run shutdown in new event loop since main loop is closing
        try:
            asyncio.run(shutdown_all_processes())
        except Exception as e:
            print(f"Error during shutdown: {e}")
        log_event("TASKMASTER_SHUTDOWN", reason="KeyboardInterrupt")
        print("Taskmaster shutdown complete.")

