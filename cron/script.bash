#!/usr/bin/python
source activate radars
/home/sepud/projetos/radares/src/scrape.py | awk '{ print strftime("%c: "), $0; fflush(); }' > /home/sepud/projetos/radares/log/output.log
# 35 17 * * * bash /home/sepud/projetos/radares/cron/script.bash