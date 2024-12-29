#
# change prompt:
#
# NOTE: this is really handy to set to differeniate from
# local cmd-line prompt. Also, if you are running client
# and server containers, helpful to have one prompt set
# to "docker-client> " and the other to "docker-server> "
#
PS1="docker> "

#
# change path to allow execution from current directory:
#
export PATH=.:$PATH

#
# turn off bash command history:
#
unset HISTFILE

#
# command aliases:
#
alias rm="rm -i"
