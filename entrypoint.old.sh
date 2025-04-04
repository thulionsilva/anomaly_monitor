#!/bin/bash

[ -f /src/models/PIC11151A.pkl ] || python /src/train.py

#Apaga toda as tarefas do crontab para evitar duplicado
crontab -r 2>/dev/null

INTERVAL=$CRON_INTERVAL
# Certifique-se de que a variável CRON_INTERVAL está definida
if [ -z "$CRON_INTERVAL" ]; then
  INTERVAL="*/15 * * * *"
fi

# Defina o comando que você deseja executar
COMMAND="python /src/predict.py > /src/crontab/predict.log 2>&1"

# Pasta de logs
mkdir -p /src/crontab

# Adicione a tarefa ao crontab
(crontab -l 2>/src/crontab/log; echo "$INTERVAL $COMMAND") | crontab -

# Listar os jobs do crontab
echo "Jobs atuais no crontab:"
crontab -l

crond -f
