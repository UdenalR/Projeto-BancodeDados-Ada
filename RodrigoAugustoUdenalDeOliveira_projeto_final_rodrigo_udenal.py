import psycopg2
import csv

conn = psycopg2.connect("host=localhost dbname=projeto_final_rodrigo_udenal user=postgres password=937739")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS regioes (
        id SERIAL PRIMARY KEY,
        noc VARCHAR(3),
        regiao VARCHAR(255),
        notes VARCHAR(255),
        CONSTRAINT unico_noc_region UNIQUE (noc, regiao)
    );
""")

with open('noc_regions.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        cur.execute("""
            INSERT INTO regioes (noc, regiao, notes)
            VALUES (%s, %s, %s)
            ON CONFLICT ON CONSTRAINT unico_noc_region DO NOTHING;
        """, (row[0], row[1], row[2]))

cur.execute("""
    CREATE TABLE IF NOT EXISTS times (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(50),
        noc VARCHAR(50),
        CONSTRAINT unico_team_noc UNIQUE (nome, noc)
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS atletas (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        genero CHAR(1),
        idade INTEGER,
        altura NUMERIC,
        peso NUMERIC,
        noc VARCHAR(3),
        time_id INTEGER REFERENCES times(id),
        CONSTRAINT unico_nome_noc UNIQUE (nome, noc)
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS esportes (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(50),
        temporada VARCHAR(6),
        regiao_id INTEGER REFERENCES regioes(id),
        CONSTRAINT unico_nome_temporada_regiao_id UNIQUE (nome, temporada, regiao_id)
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS modalidades (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(100),
        esportes_id INTEGER REFERENCES esportes(id),
        CONSTRAINT unico_nome_esportes_id UNIQUE (nome, esportes_id)
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS medalhas (
        id SERIAL PRIMARY KEY,
        atletas_id INTEGER REFERENCES atletas(id),
        modalidades_id INTEGER REFERENCES modalidades(id),
        medalha VARCHAR(10),
        ano INTEGER,
        CONSTRAINT unico_atletas_modalidades UNIQUE (atletas_id, modalidades_id)
    );
""")

with open('athlete_events.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)  # Pular o cabeçalho
    for row in reader:
        # Verificar se há "NA" em colunas específicas
        if 'NA' not in [row[3], row[4], row[5]]:
            idade = int(row[3]) if row[3] != 'NA' else None

            cur.execute("""
            INSERT INTO times (nome, noc)
            VALUES (%s, %s)
            ON CONFLICT ON CONSTRAINT unico_team_noc DO NOTHING;
            """, (row[6], row[8]))

            cur.execute("""
                INSERT INTO atletas (nome, genero, idade, altura, peso, noc, time_id)
                VALUES (%s, %s, %s, %s, %s, %s, (SELECT id FROM times WHERE nome = %s AND noc = %s LIMIT 1))
                ON CONFLICT ON CONSTRAINT unico_nome_noc DO NOTHING;
            """, (row[1], row[2], idade, row[4], row[5], row[7], row[6], row[8]))

            cur.execute("""
                INSERT INTO esportes (nome, temporada, regiao_id)
                VALUES (%s, %s, (SELECT id FROM regioes WHERE noc = %s LIMIT 1))
                ON CONFLICT ON CONSTRAINT unico_nome_temporada_regiao_id DO NOTHING;
            """, (row[12], row[10], row[7]))

            cur.execute("""
                INSERT INTO modalidades (nome, esportes_id)
                VALUES (%s, (SELECT id FROM esportes WHERE nome = %s AND temporada = %s LIMIT 1))
                ON CONFLICT ON CONSTRAINT unico_nome_esportes_id DO NOTHING;
            """, (row[13], row[12], row[10]))

            cur.execute("""
                INSERT INTO medalhas (atletas_id, modalidades_id, medalha, ano)
                VALUES (
                    (SELECT id FROM atletas WHERE nome = %s AND noc = %s LIMIT 1),
                    (SELECT id FROM modalidades WHERE nome = %s AND esportes_id = (SELECT id FROM esportes WHERE nome = %s AND temporada = %s LIMIT 1) LIMIT 1),
                    %s, %s
                ) ON CONFLICT ON CONSTRAINT unico_atletas_modalidades DO NOTHING;
            """, (row[1], row[7], row[13], row[12], row[10], row[14], row[9]))

conn.commit()
cur.close()
conn.close()

'''Consultas:

#1. Quantas medalhas cada país conseguiu no total desde 1990?

SELECT
    atletas.noc,
    COUNT(*) AS total_medalhas
FROM
    medalhas
JOIN
    atletas ON medalhas.atletas_id = atletas.id
WHERE
    medalhas.ano >= 1990
GROUP BY
    atletas.noc
ORDER BY
    total_medalhas DESC;
	
#2. TOP 3 atletas que ganharam mais medalhas de ouro? TOP 3 medalhas de prata? TOP3 medalhas de bronze?

#Top 3 Medalhas de Ouro:

SELECT
    atletas.nome,
    COUNT(*) AS total_ouro
FROM
    medalhas
JOIN
    atletas ON medalhas.atletas_id = atletas.id
WHERE
    medalhas.medalha = 'Gold'
GROUP BY
    atletas.nome
ORDER BY
    total_ouro DESC
LIMIT 3;
#Top 3 Medalhas de Prata:

SELECT
    atletas.nome,
    COUNT(*) AS total_prata
FROM
    medalhas
JOIN
    atletas ON medalhas.atletas_id = atletas.id
WHERE
    medalhas.medalha = 'Silver'
GROUP BY
    atletas.nome
ORDER BY
    total_prata DESC
LIMIT 3;

#Top 3 Medalhas de Bronze:

SELECT
    atletas.nome,
    COUNT(*) AS total_bronze
FROM
    medalhas
JOIN
    atletas ON medalhas.atletas_id = atletas.id
WHERE
    medalhas.medalha = 'Bronze'
GROUP BY
    atletas.nome
ORDER BY
    total_bronze DESC
LIMIT 3;

#3. Qual a lista de todas as modalidades existentes? A partir de que ano elas foram introduzidas nas Olimpíadas?

SELECT
    modalidades.nome AS modalidade,
    MIN(medalhas.ano) AS ano_introducao
FROM
    modalidades
JOIN
    medalhas ON modalidades.id = medalhas.modalidades_id
GROUP BY
    modalidades.nome
ORDER BY
    ano_introducao;
	
#4. Quantas medalhas de ouro, prata e bronze cada país ganhou no vôlei (tanto masculino, quanto feminino)? Não é necessário mostrar países que nunca ganharam uma medalha no esporte.

SELECT
    atletas.noc,
    COUNT(CASE WHEN medalhas.medalha = 'Gold' THEN 1 END) AS ouro,
    COUNT(CASE WHEN medalhas.medalha = 'Silver' THEN 1 END) AS prata,
    COUNT(CASE WHEN medalhas.medalha = 'Bronze' THEN 1 END) AS bronze
FROM
    medalhas
JOIN
    atletas ON medalhas.atletas_id = atletas.id
JOIN
    modalidades ON medalhas.modalidades_id = modalidades.id
JOIN
    esportes ON modalidades.esportes_id = esportes.id
JOIN
    regioes ON esportes.regiao_id = regioes.id
WHERE
    esportes.nome = 'Volleyball'
GROUP BY
    atletas.noc
HAVING
    COUNT(CASE WHEN medalhas.medalha IN ('Gold', 'Silver', 'Bronze') THEN 1 END) > 0;
	
	
# 5. Qual a média de atletas por ano a partir de 1920 (separar verão de inverno)?

SELECT
    temporada,
    ano,
    ROUND(AVG(total_atletas)) AS media_atletas
FROM (
    SELECT
        esportes.temporada,
        medalhas.ano,
        COUNT(DISTINCT atletas.id) AS total_atletas
    FROM
        medalhas
    JOIN
        atletas ON medalhas.atletas_id = atletas.id
    JOIN
        modalidades ON medalhas.modalidades_id = modalidades.id
    JOIN
        esportes ON modalidades.esportes_id = esportes.id
    WHERE
        medalhas.ano >= 1920
    GROUP BY
        esportes.temporada, medalhas.ano
) AS subquery
GROUP BY
    temporada, ano
ORDER BY
    temporada, ano;
	
	
# 6. Proporção de homens e mulheres antes e depois de 1950 (compare e explique).

SELECT
    atletas.genero, #Seleciona a coluna gênero da tabela atletas.
    COUNT(CASE WHEN medalhas.ano < 1950 THEN 1 END) AS total_antes_1950, #Conta o número de casos em que o ano da medalha é anterior a 1950 por gênero.
    COUNT(CASE WHEN medalhas.ano >= 1950 THEN 1 END) AS total_depois_1950, #Conta o número de casos em que o ano da medalha é igual ou posterior a 1950 por gênero.
    ROUND(COUNT(CASE WHEN medalhas.ano < 1950 THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN medalhas.ano >= 1950 THEN 1 END), 0), 2) AS proporcao_antes_1950 #Calcula a proporção de medalhas antes de 1950 em relação ao total de medalhas após 1950.
FROM
    atletas #Da tabela atletas.
JOIN
    medalhas ON atletas.id = medalhas.atletas_id #Une as tabelas atletas e medalhas com base na correspondência entre atletas.id e medalhas.atletas_id.
WHERE
    atletas.genero IS NOT NULL #Filtra apenas registros com a informação de gênero.
GROUP BY
    atletas.genero; #Agrupa os resultados pela coluna gênero.

'''
