from configparser import ConfigParser


def config(filename="../database.ini", section="postgresql"):
    """
        A função config() lê o arquivo database.ini e retorna um dicionário com as informações de conexão. A função config() verifica se a seção postgresql existe no arquivo database.ini e retorna um dicionário com as informações de conexão se a seção existir. Caso contrário, a função levanta uma exceção.
    """
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            "Seção {0} não encontrada no arquivo {1}".format(section, filename))
    return db
