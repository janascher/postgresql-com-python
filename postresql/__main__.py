"""
    Este arquivo contém as operações CRUD básicas para a entidade de usuário, incluindo a criação, atualização, exclusão e seleção de usuários.
"""

import psycopg2
from configs import config


class User:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

    @staticmethod
    def connect():
        """ 
            Conecta ao servidor de banco de dados PostgreSQL em Python.
        """
        conn = None
        try:
            params = config()
            print("Conectando ao banco de dados PostgreSQL...")
            conn = psycopg2.connect(**params)
            print("Conexão bem-sucedida!")
            return conn
            # conn.close()
            #print("Conexão com o banco de dados fechada.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    @staticmethod
    def create_table():
        """
            Cria a Tabela Users no banco de dados PostgreSQL
        """
        conn = User.connect()
        cur = conn.cursor()

        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY NOT NULL,
                    name VARCHAR(100) NOT NULL CHECK (name ~ '^[A-Za-zÀ-ÖØ-öø-ÿ]+$' AND name != ''),
                    email VARCHAR(100) NOT NULL CHECK (email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' AND email != '')
                );
            """)
            conn.commit()
            print("Tabela criada com sucesso!")
            cur.close()
            conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    @staticmethod
    def create_user(name, email):
        """
            Cria um usuário no banco de dados PostgreSQL
        """
        conn = User.connect()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                    INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id;
                """, (name, email)
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            print("Usuário criado com sucesso!")
            cur.close()
            conn.close()
            return User(new_id, name, email)
        except (Exception, psycopg2.DatabaseError, psycopg2.IntegrityError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    @staticmethod
    def update_user(id, name, email):
        """
            Atualiza um usuário no banco de dados PostgreSQL
        """
        conn = User.connect()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                    UPDATE users SET name=%s, email=%s WHERE id=%s RETURNING id;
                """, (name, email, id)
            )
            rows_updated = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            if rows_updated <= 0:
                raise ValueError(f"Usuário com ID={id} não encontrado.")
            else:
                print("Usuário atualizado!")
                return User(id, name, email)
        except (Exception, psycopg2.DatabaseError, psycopg2.IntegrityError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    @staticmethod
    def delete_user(id):
        """
            Deleta um usuário no banco de dados PostgreSQL
        """
        conn = User.connect()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                    DELETE FROM users WHERE id=%s;
                """, (id)
            )
            conn.commit()
            print("Usuário deletado!")
            cur.close()
            conn.close()
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    @staticmethod
    def get_all():
        """
            Pesquisa todos os usuários criados no banco de dados PostgreSQL
        """
        conn = User.connect()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                    SELECT id, name, email FROM users;
                """
            )
            result = cur.fetchall()
            users = [User(*row) for row in result]
            print(f"Resultado da pesquisa: {result}")
            cur.close()
            conn.close()
            return users
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print(f"ERRO: {error}")
        finally:
            conn.close()

    @staticmethod
    def get_by_id(id):
        """
            Pesquisa um usuário específico pelo id no banco de dados PostgreSQL
        """
        conn = User.connect()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                    SELECT id, name, email FROM users WHERE id=%s;
                """, (id)
            )
            result = cur.fetchone()
            print(f"Resultado da pesquisa: {result}")
            cur.close()
            conn.close()
            if result:
                return User(*result)
            return None
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            print(error)
        finally:
            conn.close()


"""
    Estabelece a conexão com o banco de dados
"""
# User.connect()


"""
    Cria uma tabela user
"""
# User.create_table()


"""
    Cria um usuário
"""
#User.create_user("Pedro", "pneto@mail.com")


"""
    Atualiza o usuário criado
"""
#User.update_user("0", "Pedro", "pedro@mail.com")


"""
    Deleta um usuário
"""
# User.delete_user("7")


"""
    Busca o usuário com id 
"""
# User.get_by_id("6")


"""
    Busca todos os usuários com id 
"""
# User.get_all()
