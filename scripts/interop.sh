#!/bin/sh
# Interop check against real 9P clients: plan9port's 9p(1) and Inferno.
#
#   scripts/interop.sh
#
# Starts examples/hello on a free port and drives it with whichever clients
# are available:
#   PLAN9=/path/to/plan9port          (default: $PLAN9, or 9p on $PATH)
#   INFERNO_ROOT=/path/to/inferno     (needs $INFERNO_ROOT/<OS>/.../emu or
#   INFERNO_EMU=/path/to/emu           INFERNO_EMU set explicitly)
# A client that is not found is skipped, not failed.

set -u
here=$(cd "$(dirname "$0")/.." && pwd)
port=${PORT:-5641}
addr="tcp!127.0.0.1!$port"
fail=0
pass() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }

cargo build -q --manifest-path "$here/Cargo.toml" --example hello || exit 1
"$here/target/debug/examples/hello" "127.0.0.1:$port" 2>/dev/null &
server=$!
trap 'kill $server 2>/dev/null' EXIT INT TERM
sleep 1

# ── plan9port ──
p9=""
if [ -n "${PLAN9:-}" ] && [ -x "$PLAN9/bin/9p" ]; then p9="$PLAN9/bin/9p"
elif command -v 9p >/dev/null 2>&1; then p9=9p; fi
if [ -z "$p9" ]; then
	echo "skip plan9port (set PLAN9)"
else
	[ "$($p9 -a "$addr" read hello)" = "hello" ] && pass "9p read" || bad "9p read"
	$p9 -a "$addr" ls -l | grep -q 'ticks' && pass "9p ls -l" || bad "9p ls -l"
	n=$(timeout 2.5 $p9 -a "$addr" read ticks | grep -c '^tick')
	[ "$n" -ge 2 ] && pass "9p blocking read ($n ticks)" || bad "9p blocking read ($n ticks)"
fi

# ── Inferno ──
emu=${INFERNO_EMU:-}
if [ -z "$emu" ] && [ -n "${INFERNO_ROOT:-}" ]; then
	case $(uname) in Darwin) os=MacOSX ;; *) os=$(uname) ;; esac
	for e in "$INFERNO_ROOT/emu/$os/o.emu" "$INFERNO_ROOT/$os"/*/bin/emu; do
		[ -x "$e" ] && { emu=$e; break; }
	done
fi
if [ -z "$emu" ] || [ -z "${INFERNO_ROOT:-}" ]; then
	echo "skip Inferno (set INFERNO_ROOT, and INFERNO_EMU if emu is not found)"
else
	script=$(mktemp)
	cat >"$script" <<EOF
mount -A $addr /n/client
cat /n/client/hello
{echo three little words >[1=0]; cat} <> /n/client/query
cat /n/client/ticks > /tmp/styx9p-a &
pa=\$apid
cat /n/client/ticks > /tmp/styx9p-b &
pb=\$apid
sleep 3
echo kill > /prog/\$pa/ctl
echo kill > /prog/\$pb/ctl
sleep 1
echo a \`{cat /tmp/styx9p-a | wc -l}
echo b \`{cat /tmp/styx9p-b | wc -l}
cat /n/client/hello
rm /tmp/styx9p-a /tmp/styx9p-b
EOF
	start=$(date +%s)
	out=$(timeout 30 "$emu" -r"$INFERNO_ROOT" /dis/sh.dis "#U*$script" </dev/null 2>&1)
	took=$(($(date +%s) - start))
	rm -f "$script"
	echo "$out" | grep -q '^hello$' && pass "inferno mount + read" || bad "inferno mount + read"
	echo "$out" | grep -q '^3 words$' && pass "inferno write-then-read on one fid" || bad "inferno query"
	a=$(echo "$out" | awk '$1=="a"{print $2}'); b=$(echo "$out" | awk '$1=="b"{print $2}')
	[ "${a:-0}" -ge 2 ] && [ "${b:-0}" -ge 2 ] && pass "inferno two readers, one mount ($a, $b ticks)" \
		|| bad "inferno two readers ($a, $b ticks)"
	[ "$(echo "$out" | grep -c '^hello$')" -eq 2 ] && [ "$took" -lt 10 ] \
		&& pass "inferno kill of blocked readers (Tflush) in ${took}s, mount still healthy" \
		|| bad "inferno Tflush (${took}s): $out"
fi

exit $fail
