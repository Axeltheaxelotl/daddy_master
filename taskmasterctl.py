import socket
import readline
import os

SOCKET_PATH = "/tmp/simple.sock"

def connect_to_daemon():
    """Connect to the daemon with error handling"""
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(SOCKET_PATH)
        return sock
    except ConnectionRefusedError:
        print("Cannot connect to taskmaster daemon. Is it running?")
        return None
    except FileNotFoundError:
        print(f"Socket file {SOCKET_PATH} not found. Is taskmaster daemon running?")
        return None
    except Exception as e:
        print(f"Connection error: {e}")
        return None

sock = connect_to_daemon()
if not sock:
    exit(1)

def send_command(command, sock):
    """Send command with connection error handling"""
    try:
        sock.send(command.encode())
        
        # Read response in chunks until we get all data
        response_parts = []
        while True:
            try:
                # Set a short timeout to avoid hanging
                sock.settimeout(0.1)
                chunk = sock.recv(4096).decode()
                if not chunk:
                    break
                response_parts.append(chunk)
            except socket.timeout:
                # No more data available
                break
            except Exception:
                break
        
        # Reset to blocking mode
        sock.settimeout(None)
        return ''.join(response_parts).strip(), True  # Success
    except ConnectionResetError:
        return "Connection lost to daemon", False
    except BrokenPipeError:
        return "Connection broken to daemon", False
    except Exception as e:
        return f"Communication error: {e}", False

def completer(text, state):
    """Tab completion for common commands"""
    commands = ['start', 'stop', 'restart', 'status', 'reload', 'tail', 'attach', 'clear-logs', 'clear', 'help', 'exit', 'quit', 'shutdown']
    options = [cmd for cmd in commands if cmd.startswith(text)]
    if state < len(options):
        return options[state]
    return None

def reconnect_if_needed():
    """Try to reconnect to daemon if connection is lost"""
    global sock
    if sock:
        try:
            sock.close()
        except:
            pass
    sock = connect_to_daemon()
    return sock is not None

def main():
    global sock
    readline.parse_and_bind('tab: complete')
    readline.set_completer(completer)
    
    print("Taskmaster Control Shell")
    print("Type 'help' for available commands, 'exit' to quit.")
    print()
    
    try:
        prompt = "taskmasterctl> "
        while True:
            command = input(prompt).strip()
            if command.lower() in ['exit', 'quit']:
                print("Exiting taskmasterctl.")
                break
            if not command:
                continue
            
            # Handle local commands
            if command.lower() == 'clear':
                os.system('clear' if os.name == 'posix' else 'cls')
                continue
            
            # Handle shutdown specially
            if command.lower() == 'shutdown':
                print("Sending shutdown command to daemon...")
                response, success = send_command(command, sock)
                print(response)
                if success:
                    print("Daemon shutting down. Exiting client.")
                    break
                else:
                    print("Failed to send shutdown command.")
                continue
            
            # Handle attachment mode specially
            if command.lower().startswith('attach '):
                program = command.split()[1] if len(command.split()) > 1 else None
                if program:
                    print(f"Attaching to {program}... (Press Ctrl+C to detach)")
                    try:
                        sock.send(command.encode())
                        # Read attachment response
                        while True:
                            try:
                                response = sock.recv(4096).decode()
                                if not response:
                                    print("Connection lost during attachment")
                                    if not reconnect_if_needed():
                                        return
                                    break
                                print(response, end='')
                            except KeyboardInterrupt:
                                print("\nDetaching...")
                                try:
                                    sock.send(b"\x03")  # Send Ctrl+C
                                except:
                                    pass
                                break
                    except Exception as e:
                        print(f"Attachment error: {e}")
                        if not reconnect_if_needed():
                            return
                else:
                    print("Usage: attach <program>")
            else:
                response, success = send_command(command, sock)
                print(response)
                if not success:
                    print("Attempting to reconnect...")
                    if not reconnect_if_needed():
                        print("Failed to reconnect. Exiting.")
                        return
                    print("Reconnected successfully.")
                
    except KeyboardInterrupt:
        print()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if sock:
            try:
                sock.close()
            except:
                pass

if __name__ == "__main__":
    import sys
    
    # Check for -c flag for single command execution
    if len(sys.argv) == 3 and sys.argv[1] == "-c":
        command = sys.argv[2]
        sock = connect_to_daemon()
        if not sock:
            exit(1)
        
        # Handle special commands
        if command.startswith("attach "):
            parts = command.split(None, 1)
            if len(parts) == 2:
                program_name = parts[1]
                response, success = send_command(f"attach {program_name}", sock)
                if success and "Attaching to" in response:
                    print(response)
                    try:
                        while True:
                            data = sock.recv(4096).decode()
                            if not data:
                                break
                            print(data, end='')
                    except KeyboardInterrupt:
                        print("\nDetached from process.")
                else:
                    print(response)
            else:
                print("Usage: attach <program>")
        else:
            # Regular command
            response, success = send_command(command, sock)
            print(response)
            if not success:
                exit(1)
        
        sock.close()
    else:
        # Interactive mode
        main()