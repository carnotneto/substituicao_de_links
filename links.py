import pymysql

# Configurações do banco de dados
DB_HOST = 'localhost'
DB_USER = 'seu_usuario'
DB_PASSWORD = 'sua_senha'
DB_NAME = 'seu_banco_de_dados'
TABLE_NAME = 'sua_tabela'
COLUMN_NAME = 'sua_coluna'

STRING_BUSCA = 'https://player.vimeo.com/video/'
TEMPLATE_SUBSTITUICAO = 'https://beyond.spalla.io/player/?video=3333149b-3333-4bb6-aaaa-000{}'

def main():
    # Conecta ao banco de dados
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # Seleciona os registros que precisam ser atualizados
            sql_select = f"SELECT id, {COLUMN_NAME} FROM {TABLE_NAME} WHERE {COLUMN_NAME} LIKE %s"
            cursor.execute(sql_select, ('%' + STRING_BUSCA + '%'))
            rows = cursor.fetchall()

            for row in rows:
                id = row['id']
                dados_coluna = row[COLUMN_NAME]

                # Extrai o ID do Vimeo
                inicio_indice = dados_coluna.find(STRING_BUSCA) + len(STRING_BUSCA)
                fim_indice = dados_coluna.find('?', inicio_indice)
                id_vimeo = dados_coluna[inicio_indice:fim_indice]

                # Constrói a nova URL de substituição
                nova_url = TEMPLATE_SUBSTITUICAO.format(id_vimeo)

                # Substitui o conteúdo dentro das aspas do src
                novos_dados_coluna = dados_coluna.replace(
                    f'{STRING_BUSCA}{id_vimeo}?badge=0&amp;autopause=0&amp;player_id=0&amp;app_id=58479',
                    nova_url
                )

                # Atualiza o registro no banco de dados
                sql_update = f"UPDATE {TABLE_NAME} SET {COLUMN_NAME} = %s WHERE id = %s"
                cursor.execute(sql_update, (novos_dados_coluna, id))

            connection.commit()
            print("Banco de dados atualizado com sucesso.")

    except Exception as e:
        print(f"Erro ao atualizar o banco de dados: {e}")

    finally:
        connection.close()

if __name__ == '__main__':
    main()
