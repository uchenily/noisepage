# server side:
#     gdbserver :1234 build/bin/noisepage

target remote noisepage:1234
# break SimpleQueryCommand::Exec
# break Taskflow::BindQuery
# break ExecutePortal
break Taskflow::ExecuteCreateStatement
tui enable
continue
