import os,sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.IMDBExtractor import get_files,process_files

dfs = process_files(get_files("../../Spark/data/"),"../../Spark/data/")
print(dfs)