source activate radars
/home/sepud/projetos/radares/src/scrape.py | awk '{ print strftime("%c: "), $0; fflush(); }' > /home/sepud/projetos/radares/log/output.log

# CRONTAB LINE */2 * * * * bash /home/sepud/projetos/radares/cron/script.bash

