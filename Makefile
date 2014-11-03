.PHONY: reset_rabbitmq test


reset_rabbitmq:
	sudo rabbitmqctl stop_app
	sudo rabbitmqctl reset
	sudo rabbitmqctl start_app
	sudo rabbitmqctl add_vhost bluelink
	sudo rabbitmqctl set_permissions -p bluelink guest ".*" ".*" ".*"


test:
	sudo service rabbitmq-server start
	sudo rabbitmqctl delete_vhost "/aioamqptest" || true
	sudo rabbitmqctl add_vhost "/aioamqptest"
	sudo rabbitmqctl set_permissions -p /aioamqptest guest ".*" ".*" ".*"
	TRAVIS=true PYTHONASYNCIODEBUG=1 RABBITMQCTL_CMD="sudo rabbitmqctl" nosetests --verbosity=2 aioamqp
