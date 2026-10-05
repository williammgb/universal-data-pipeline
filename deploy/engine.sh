# The Docker engine, sourced by ./run and ./udp: inside WSL on this Windows machine, the
# machine's own on Linux. Decided once here, so the gate a person runs is the same gate a pull
# request runs, and `./udp up` starts the platform on the same engine the gates use.
if command -v wsl.exe > /dev/null 2>&1; then engine="wsl.exe -e"; else engine=; fi

# WSL shuts the distro down about 15 seconds after the last wsl.exe command ends,
# stopping every container with it — even while tests on Windows are still using
# the database. One idle wsl.exe session held open for the life of the stack
# keeps it up. Without WSL there is nothing to keep up.
# keepalive_start <name> <how long: seconds, or infinity>
keepalive_start() {
  [ -n "$engine" ] || return 0
  MSYS_NO_PATHCONV=1 wsl.exe -e sh -c \
    "echo \$\$ > /tmp/$1.keepalive; exec sleep $2" > /dev/null 2>&1 &
}

# keepalive_stop <name>
keepalive_stop() {
  [ -n "$engine" ] || return 0
  MSYS_NO_PATHCONV=1 wsl.exe -e sh -c \
    "kill \$(cat /tmp/$1.keepalive) 2>/dev/null; rm -f /tmp/$1.keepalive"
}
