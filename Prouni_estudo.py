# Databricks notebook source
# MAGIC %md
# MAGIC No Brasil, os cursos de medicina são extremamente concorridos e exigentes, com uma carga horária intensa e duração média de seis anos. As faculdades de medicina no país oferecem formação em diversas áreas, como cirurgia, pediatria, ginecologia e obstetrícia, entre outras especialidades médicas.
# MAGIC
# MAGIC Existem opções de bolsas para estudantes que desejam cursar medicina no país. Uma das principais opções é o Programa Universidade para Todos (PROUNI), que oferece bolsas de estudos parciais e integrais em universidades particulares para estudantes de baixa renda que tenham obtido boas notas no Enem.
# MAGIC
# MAGIC Além disso, há também a possibilidade de ingressar em universidades públicas, que são gratuitas e oferecem um ensino de qualidade reconhecido mundialmente. No entanto, o processo seletivo para ingresso em universidades públicas é bastante concorrido e exige um alto desempenho dos candidatos.
# MAGIC
# MAGIC Nas próximas seções, iremos disponibilizar alguns resultados com base em dados modificados do PROUNI e de universidades públicas, a fim de fornecer informações mais detalhadas sobre as opções de bolsas e ingresso nessas instituições de ensino.

# COMMAND ----------

local_do_arquivo = "/FileStore/tables/cursos_prouni.csv"


# COMMAND ----------

# DBTITLE 1,Its important to define a column as index to increase read performance
import pyspark.pandas as ps

df = ps.read_csv(local_do_arquivo, index_col='curso_id')

# COMMAND ----------

df.head()

# COMMAND ----------

df.shape

# COMMAND ----------

# DBTITLE 1,How to check if two the content of two columns are equal
df['curso_busca'].equals(df['nome']).sum() # sum count how much trues exist

# COMMAND ----------

# DBTITLE 1,How to drop a column (axis=1)
df = df.drop(['curso_busca'], axis=1)

# COMMAND ----------

# DBTITLE 1,How to rename column and multiple columns
df = df.rename(columns={'nome': 'nome_curso'})

# COMMAND ----------

# DBTITLE 1,How to count the course = Medicine
df[df['nome_curso']=='Medicina']['nome_curso'].count()

# COMMAND ----------

# DBTITLE 1,How to distinct the courses
df['turno'].unique()

# COMMAND ----------

# DBTITLE 1,How much courses of Medicine is Integral period
df[(df['turno']=='Integral') & (df['nome_curso'] == 'Medicina')]['nome_curso'].count()

# COMMAND ----------

# DBTITLE 1,How much courses of Medicine is Integral period
df[(df['turno']=='Integral') & (df['nome_curso'] == 'Medicina') | (df['turno']=='Matutino') & (df['nome_curso'] == 'Medicina')]['nome_curso'].count()

# COMMAND ----------

local_do_arquivo = '/FileStore/tables/reajuste.csv'
df_mensalidade = ps.read_csv(local_do_arquivo)

# COMMAND ----------

df_mensalidade.head()

# COMMAND ----------

df_mensalidade.shape

# COMMAND ----------

ps.set_option('compute.ops_on_diff_frames', True) # it not work if the dataframes has not the same index.

# COMMAND ----------

df.reset_index(inplace=True) # its not a good pratice, but if both dataframes has no index, it works

# COMMAND ----------

df['mensalidade'] = df['mensalidade']+df_mensalidade['reajuste'] # Its not possible to combine columns from two different datasets
#Its not computationally efficient, because it can be on differente nodes etc.We can do it, but is not a good pratice


# COMMAND ----------

# DBTITLE 1,How to resume a data frame ? First, drop null values to avoid interference in the resume
df.dropna(inplace=True)
df.head()

# COMMAND ----------

df.describe()

# COMMAND ----------

df_descricao = df[df['nome_curso'] == 'Medicina'].describe()

# COMMAND ----------

df_descricao_bolsa = df_descricao.drop(['mensalidade','curso_id','campus_id',"nota_integral_ampla", "nota_integral_cotas", "nota_parcial_ampla", "nota_parcial_cotas"],axis=1)

# COMMAND ----------

df_descricao_bolsa = df_descricao_bolsa.drop(['count'], axis=0)

# COMMAND ----------

df_descricao_bolsa

# COMMAND ----------

df_descricao_bolsa.style.format('{:,.2f}')

# COMMAND ----------

df_descricao_bolsa.style.format('{:,.2f}').background_gradient(axis=1)

# COMMAND ----------

# DBTITLE 1,Part of other study
import pyspark.pandas as ps

# Crie um DataFrame de exemplo
data = {
    'App': ['App_A', 'App_B', 'App_C', 'App_D', 'App_E'],
    'Avaliação': [3.5, 4.2, 3.8, 4.5, 2.9],
    'Downloads': [15000, 23000, 18000, 27000, 10000]
}

df_teste_externo = ps.DataFrame(data)
df_teste_externo

# COMMAND ----------

df_medicina = df.query("nome_curso == 'Medicina'")
df_medicina.head()

# COMMAND ----------

df_medicina.query("turno == 'Integral' and cidade_busca =='Sao Paulo'")

# COMMAND ----------

agrupado = df_medicina.query("turno == 'Integral' or turno == 'Matutino'").groupby('uf_busca')['mensalidade'].mean()
# como "Integral" e "Matutino" são as únicas opções de turno atualmente, o retorno será todos os cursos
# vamos agrupar as informações por estado com o método groupby(). Como parâmetro, passaremos a coluna "uf_busca". Então, selecionaremos os dados de mensalidade e calcularemos a média, # # com o método mean()

# COMMAND ----------

df_agrupado = ps.DataFrame(agrupado)


# COMMAND ----------

df_agrupado

# COMMAND ----------

df_agrupado.sort_values(by='mensalidade', inplace=True, ascending=False)

# COMMAND ----------

df_agrupado

# COMMAND ----------

df_agrupado = df_agrupado.reset_index()

# COMMAND ----------

# DBTITLE 1,Koalas uses the ploty lib instead matplotlib
df_agrupado.plot.bar(x='uf_busca', y='mensalidade')

# COMMAND ----------

ps.sql('''
       select bolsa_integral_cotas, uf_busca
       from {DF}
       where nome_curso = 'Medicina'
       limit 5
       ''', DF=df)

# COMMAND ----------

cotas_ordenadas = ps.sql('''
       select uf_busca, sum(bolsa_integral_cotas) as total_de_cotas
       from {DF}
       where nome_curso = 'Medicina'
       group by uf_busca
       order by total_de_cotas desc
       ''', DF=df)

# COMMAND ----------

cotas_ordenadas.head()

# COMMAND ----------

cotas_ordenadas.plot.bar(x='uf_busca', y='total_de_cotas')

# COMMAND ----------

# DBTITLE 1,Tenho mais cotas em capitais de estado ou em outros municípios ?
df_medicina['cidade_busca'].unique()

# COMMAND ----------

nomes_cidades = {
    "Sao Luis": "Capital",
    "Joao Pessoa":"Capital",
    "Belem":"Capital",
    "Itaperuna":"Município",
    "Vitoria":"Município",
    "Franca":"Município",
    "Sao Paulo":"Capital",
    "Guarapuava":"Município",
    "Campo Mourao":"Município",
    "Montes Claros":"Município",
    "Rio Branco":"Município",
    "Imperatriz":"Município",
    "Vespasiano":"Município",
    "Porto Velho": "Município"
}

# COMMAND ----------

capitais = df_medicina['cidade_busca'].map(nomes_cidades)

# COMMAND ----------

df_medicina.insert(loc=8, column='tipo_cidade', value=capitais)

# COMMAND ----------

df_medicina.head()

# COMMAND ----------

cotas_ordenadas = ps.sql('''
                         select tipo_cidade, sum(bolsa_integral_cotas) as total_de_cotas
                         from {DF}
                         group by tipo_cidade
                         order by total_de_cotas
                         ''', DF=df_medicina)
cotas_ordenadas.plot.bar(x='tipo_cidade', y='total_de_cotas')

# COMMAND ----------

# DBTITLE 1,Como a mensalidade do curso se relaciona com a quantidade de bolsas?

aux = df_medicina[['mensalidade','bolsa_integral_cotas','bolsa_integral_ampla','bolsa_parcial_cotas','bolsa_parcial_ampla','nota_integral_ampla', 'nota_integral_cotas', 'nota_parcial_ampla','nota_parcial_cotas']]

# COMMAND ----------

aux.corr().style.background_gradient(cmap='Reds', vmin=-1, vmax=1)

# COMMAND ----------

# DBTITLE 1,Correlação da mensalidade X bolsa parcial cotas
df_medicina.plot.scatter(x='mensalidade', y='bolsa_parcial_cotas', trendline='ols', trendline_color_override='red')

# COMMAND ----------

df_medicina.plot.scatter(x='nota_parcial_ampla', y='nota_parcial_cotas', trendline='ols', trendline_color_override='red')

# COMMAND ----------

# DBTITLE 1,Como o turno do curso afeta a mensalidade ?
df[df['turno'] == 'Integral']['mensalidade'].plot.box()

# COMMAND ----------

df[df['turno'] == 'Matutino']['mensalidade'].plot.box()

# COMMAND ----------

df[df['nome_curso'] == 'Medicina']['mensalidade'].plot.box()

# COMMAND ----------

df[df['nome_curso'] == 'Enfermagem']['mensalidade'].plot.box()

# COMMAND ----------

import pyspark.pandas as ps

data = {'Produto': ['A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C'],
        'Preço (R$)': [1200, 1500, 1000, 1300, 1600, 1100, 1250, 1550, 1050, 1350, 1450, 1150]}

df_teste = ps.DataFrame(data)
df_teste

# COMMAND ----------

df_teste['Preço (R$)'].plot.box()

# COMMAND ----------

# DBTITLE 1,Strings
df_medicina['universidade_nome'].unique()

# COMMAND ----------

selecao = df_medicina[df_medicina['universidade_nome'].str.contains('Universidade')][['universidade_nome']]


# COMMAND ----------

selecao

# COMMAND ----------

df_medicina['universidade_nome'].loc[42] = 'Centro Universitário Integrado de Campo Mourão = CUIM'
df_medicina['universidade_nome'].loc[42]

# COMMAND ----------

separado = df_medicina['universidade_nome'].str.split(' - ', n=1, expand=True)
separado

# COMMAND ----------

df_medicina.insert(loc=12,column='sigla',value=separado[1])


# COMMAND ----------

df_medicina.head()

# COMMAND ----------

notas_ordenadas = ps.sql('''
SELECT sigla, MEAN(nota_integral_ampla) AS nota_media
from {DF}
group by sigla
order by nota_media desc
''', DF=df_medicina)
notas_ordenadas.plot.bar(x='sigla',y='nota_media')


# COMMAND ----------

# DBTITLE 1,Dados externos
url = 'https://pt.wikipedia.org/wiki/Ensino_superior_no_Brasil'

# COMMAND ----------

!pip install lxml

# COMMAND ----------

len(lista)

# COMMAND ----------

lista = ps.read_html(url)

# COMMAND ----------

lista[0]

# COMMAND ----------

df_publicas = lista[0]
df_publicas

# COMMAND ----------

type(df_publicas)

# COMMAND ----------

df_publicas.sort_values(by=['Classificação Nacional'],inplace=True, ascending=False)

# COMMAND ----------

fig = df_publicas.plot.bar(x='sigla',y='Classificação Nacional')
fig.show()

# COMMAND ----------

fig = df_publicas.plot.bar(x='sigla',y='Classificação Nacional',color=range(10))
fig.show()

# COMMAND ----------

fig = df_publicas.plot.bar(x='sigla',y='Classificação Nacional',color=range(10))
fig.layout.coloraxis.showscale = False
fig.show()
