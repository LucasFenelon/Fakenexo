Url para instalçaõ do Spark standalone em ambietnes Windows (https://phoenixnap.com/kb/install-spark-on-windows-10)

##1 - Para execução dos testes, via linha de comando um de dois
     dois arquivos devem ser executados, sendo eles:
		- "python biosales.py"
		- ou "pytest -q test_biosales.py"
		
##2 - Lembrando que apenas um dos comandos devem ser apenas uma
     vez executados, pois criam a pasta "results", e para uma 
	 segunda execução a pasta deve ser removida afim de evitar 
	 erro "caminho já existente"
	 
comando para execução do job spark de modo cluster em ambiente hadoop:

##spark-submit \
##--master yarn \
##--deploy-mode cluster \
##--conf 'spark.yarn.dist.archives=SUA_PASTA.zip#deps' \
##--conf 'spark.yarn.appMasterEnv.PYTHONPATH=deps' \
##--conf 'spark.executorEnv.PYTHONPATH=deps' \
## biosales.py

Boa sorte!
