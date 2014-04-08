#! /bin/sh

### BEGIN INIT INFO
# Provides:             pybal
# Required-Start:       $remote_fs $syslog $network
# Required-Stop:        $remote_fs $syslog
# Should-Start:         $named
# Should-Stop:
# Default-Start:        2 3 4 5
# Default-Stop:         0 1 6
# Short-Description:    PyBal
# Description:          PyBal LVS monitor
### END INIT INFO

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
DAEMON=/usr/sbin/pybal
DAEMON_OPTS=""
NAME=pybal
DESC=pybal
PIDFILE=/var/run/$NAME.pid

. /lib/lsb/init-functions

test -x $DAEMON || exit 0

if [ -f /etc/default/pybal ] ; then
	. /etc/default/pybal
fi

case "$1" in
  start)
	if pidofproc -p $PIDFILE $DAEMON > /dev/null; then
		log_failure_msg "Starting $DESC (already started)"
		exit 0
	fi
	log_daemon_msg "Starting $DESC" "$NAME"
	start-stop-daemon --start --quiet --pidfile $PIDFILE \
		--exec $DAEMON -- $DAEMON_OPTS
	log_end_msg $?
	;;
  stop)
	log_daemon_msg "Stopping $DESC" "$NAME"
	start-stop-daemon --stop --quiet --pidfile $PIDFILE \
		--name $NAME --retry 2 --oknodo
	case "$?" in
		0) log_end_msg 0 ;;
		1) log_progress_msg "(already stopped)"
		   log_end_msg 0 ;;
		*) log_end_msg 1 ;;
	esac
	;;
  reload|force-reload)
	log_daemon_msg "Reloading $DESC" "$NAME"
	start-stop-daemon --stop --quiet --signal 1 --pidfile $PIDFILE
	log_end_msg $?
	;;
  restart)
	$0 stop
	$0 start
	;;
  status)
	status_of_proc -p $PIDFILE $DAEMON $NAME && exit 0 || exit $?
	;;
  *)
	echo "Usage: ${0} {start|stop|restart|force-reload|status}" >&2
	exit 1
	;;
esac

exit 0
