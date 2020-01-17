
app=${1:-"main.py"}
cmd="python3 "${app}
echo ${cmd}
nohup  ${cmd}  >> ./logs/console.log 2>&1 &

