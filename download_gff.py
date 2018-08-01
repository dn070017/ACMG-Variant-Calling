import requests

url = 'http://genome-euro.ucsc.edu/cgi-bin/hgTables?hgsid=228771836_EtO1srCIUJm3YPB3zZZJbWWe8TM1'    
session = requests.Session()

params = {
    'hgsid': '228771836_EtO1srCIUJm3YPB3zZZJbWWe8TM1',
    'jsh_pageVertPos': '0',
    'clade': 'mammal',
    'org': 'Human',
    'db': 'hg19',
    'hgta_group': 'genes',
    'hgta_track': 'knownGene',
    'hgta_table': 'knownGene',
    'hgta_regionType': 'range',
    'position': 'chr9:21802635-21865969',
    'hgta_outputType': 'gff',
    'boolshad.sendToGalaxy': '0',
    'boolshad.sendToGreat': '0',
    'boolshad.sendToGenomeSpace': '0',
    'hgta_outFileName': '',
    'hgta_compressType': 'none',
    'hgta_doTopSubmit': 'get output'
}

response = session.post(url, data=params)
content = str(response.content)[2:-1]
decoded_string = bytes(content, "utf-8").decode('unicode_escape')
print(decoded_string)
