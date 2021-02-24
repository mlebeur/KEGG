import os, pandas, csv, re
import numpy as np
from biothings.utils.dataload import dict_convert, dict_sweep
from biothings import config
logging = config.logger
def load_KEGG(data_folder):
    infileInfo = os.path.abspath("/opt/biothings/GRCh37/kegg/april2011/KeggInfo.tsv")
    infileID = os.path.abspath("/opt/biothings/GRCh37/kegg/april2011/EnsemblToKegg.tsv")
    assert os.path.exists(infileInfo)
    assert os.path.exists(infileID)
    datInfo = pandas.read_csv(infileInfo,sep="\t",squeeze=True,quoting=csv.QUOTE_NONE)
    datID = pandas.read_csv(infileID,sep="\t",squeeze=True,quoting=csv.QUOTE_NONE)
    dat = datID.join(datInfo.set_index('kegg_id'), on='kegginfo_id').to_dict(orient='records')
    results = {}
    for rec in dat:
        _id = rec["gene_id"]
        process_key = lambda k: k.replace(" ","_").lower()
        rec = dict_convert(rec,keyfn=process_key)
        rec = dict_sweep(rec,vals=[np.nan])
        results.setdefault(_id,[]).append(rec)
    for _id,docs in results.items():
        doc = {"_id": _id, "KEGG" : docs}
        yield doc
