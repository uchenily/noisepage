# server side:
#     gdbserver :1234 build/bin/noisepage
file build/bin/noisepage
target remote noisepage:1234
# break SimpleQueryCommand::Exec
break Taskflow::BindQuery
tui enable
continue
