#!/usr/bin/python
source activate radars
python /home/sepud/projetos/radares/src/scrape.py | awk '{ print strftime("%c: "), $0; fflush(); }' > /home/sepud/projetos/radares/log/output.log