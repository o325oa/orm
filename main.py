import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Stock, Shop, Sale


def create_connection(sqlsystem, login, password, host, port, db_name):
    try:
        engine = sqlalchemy.create_engine(f'{sqlsystem}://{login}:{password}@{host}:{port}/{db_name}')
        print('Соединение установлено')
    except:
        print(f'Ошибка подключения')
    return engine


engine = create_connection('postgresql', 'postgres', '2904', 'localhost', 5432, 'postgres')
Session = sessionmaker(bind=engine)
session = Session()

create_tables(engine)

with open('tests_data.json', 'rt') as f:
    data = json.load(f)

for line in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[line.get('model')]
    session.add(model(id=line.get('pk'), **line.get('fields')))

session.commit()


def sale_list(search=input('Введите идентификатор или имя автора: ')):
    search = search
    if search.isnumeric():
        results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .join(Stock, Stock.id_book == Book.id) \
            .join(Shop, Shop.id == Stock.id_shop) \
            .join(Sale, Sale.id_stock == Stock.id) \
            .filter(Publisher.id == search).all()
        for book, shop, price, date in results:
            print(f'{book: <40} | {shop: <10} | {price: <10} | {date}')
    else:
        results = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .join(Stock, Stock.id_book == Book.id) \
            .join(Shop, Shop.id == Stock.id_shop) \
            .join(Sale, Sale.id_stock == Stock.id) \
            .filter(Publisher.name == search).all()
        for book, shop, price, date in results:
            print(f'{book: <40} | {shop: <10} | {price: <10} | {date}')


session.close()

if __name__ == '__main__':
    create_connection('postgresql', 'postgres', '2904', 'localhost', 5432, 'postgres')
    sale_list()