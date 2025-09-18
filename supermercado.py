import csv
import random

# Listas de opções para gerar dados
produtos = [
    ("Arroz", "Alimentos", 25.50),
    ("Feijão", "Alimentos", 9.80),
    ("Carne", "Alimentos", 39.90),
    ("Detergente", "Limpeza", 3.40),
    ("Amaciante", "Limpeza", 7.90),
    ("Sabão em pó", "Limpeza", 12.50),
    ("Shampoo", "Higiene", 15.90),
    ("Sabonete", "Higiene", 4.20),
    ("Papel Higiênico", "Higiene", 18.50),
    ("Refrigerante", "Bebidas", 6.20),
    ("Suco", "Bebidas", 4.50),
    ("Cerveja", "Bebidas", 3.80),
    ("Vinho", "Bebidas", 49.90)
]

cidades = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte",
    "Curitiba", "Porto Alegre", "Salvador", "Fortaleza",
    "Recife", "Brasília", "Manaus"
]

# Nome do arquivo de saída
arquivo = "supermercado.csv"

# Quantidade de registros
num_registros = 1_000_000

with open(arquivo, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    
    # Cabeçalho
    writer.writerow(["ID", "Produto", "Categoria", "Preco", "Quantidade", "Cidade"])
    
    for i in range(1, num_registros + 1):
        produto, categoria, preco = random.choice(produtos)
        quantidade = random.randint(1, 20)
        cidade = random.choice(cidades)
        
        writer.writerow([i, produto, categoria, preco, quantidade, cidade])

print(f"Arquivo '{arquivo}' gerado com {num_registros:,} registros!")
