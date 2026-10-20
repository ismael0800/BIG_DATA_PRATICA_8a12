BIG DATA - SEMANA 8 - APOIO À PRÁTICA

Ordem recomendada:
1. 01_spark_session.py
2. 02_carregar_explorar.py
3. 03_criar_silver.py
4. 04_criar_gold.py
5. 05_spark_sql.py
6. 06_salvar_verificar_parquet.py

Observação:
Os arquivos 03–06 são trechos didáticos para executar em sequência no mesmo
notebook/sessão. Para executar tudo de uma vez como script, use:
07_pipeline_completo_corrigido.py

O CSV vendas_2024.csv já acompanha este pacote.

IMPORTANTE:
A contagem de nulos deste pacote usa:
count(when(col(c).isNull(), 1))
