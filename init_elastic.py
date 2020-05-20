from utils.elastic_connector import Connector
from werkzeug.security import generate_password_hash


def init_users(con):
    user = {
        'name': 'basic_user',
        'password_hash': generate_password_hash('heslo'),
        'level': 'user',
    }
    analyst = {
        'name': 'analyst',
        'password_hash': generate_password_hash('heslo'),
        'level': 'analyst',
    }
    res = con.index(index="users", doc=user)
    print(res)
    res = con.index(index="users", doc=analyst)
    print(res)


def main():
    con = Connector()
    init_users(con)


if __name__ == '__main__':
    main()
