import csv
import random
from datetime import datetime, timedelta

produtos = [
    ("P001", "Eletrônicos", 1200.00),
    ("P002", "Acessórios", 80.00),
    ("P003", "Livros", 45.50),
    ("P004", "Móveis", 350.00),
    ("P005", "Roupas", 150.00),
]

cidades = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre", "Salvador", "Fortaleza"]

def data_randomica():
    inicio = datetime(2025, 1, 1)
    fim = datetime(2025, 12, 31)
    delta = fim - inicio
    dias_random = random.randint(0, delta.days)
    return (inicio + timedelta(days=dias_random)).strftime("%Y-%m-%d")

with open("vendas.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["ID_Pedido", "ID_Produto", "Categoria", "Preco", "Quantidade", "Data_Venda", "ID_Cliente", "Cidade"])

    for i in range(1, 500001):  # 500 mil registros
        id_pedido = 1000 + i
        produto = random.choice(produtos)
        quantidade = random.randint(1, 5)
        data_venda = data_randomica()
        id_cliente = f"C{random.randint(1, 10000):05d}"
        cidade = random.choice(cidades)

        writer.writerow([id_pedido, produto[0], produto[1], produto[2], quantidade, data_venda, id_cliente, cidade])

print("Arquivo 'vendas.csv' gerado com sucesso!")
