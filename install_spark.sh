#!/bin/bash
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' 

check_root() {
    echo -e "${YELLOW}Verificando privilégios de root...${NC}"
    if [ "$(id -u)" -ne 0 ]; then
        echo -e "${RED}Erro: Este script precisa ser executado com privilégios de root (use sudo).${NC}"
        exit 1
    fi
    echo -e "${GREEN}Verificação de privilégios concluída.${NC}"
}

install_ansible() {
    echo -e "\n${YELLOW}Instalando o Ansible Core...${NC}"
    dnf install -y ansible-core
    if [ $? -ne 0 ]; then
        echo -e "${RED}Falha ao instalar o Ansible Core. Verifique os logs do DNF.${NC}"
        exit 1
    fi
    echo -e "${GREEN}Ansible Core instalado com sucesso!${NC}"
}

verify_installation() {
    echo -e "\n${YELLOW}Verificando a instalação do Ansible...${NC}"
    if command -v ansible &> /dev/null; then
        echo -e "${GREEN}Instalação verificada com sucesso!${NC}"
        ansible --version
    else
        echo -e "${RED}Verificação falhou. O comando 'ansible' não foi encontrado no PATH.${NC}"
    fi
}

installation_spark() {
    echo -e "\n${YELLOW}Instalando o Spark...${NC}"
    ansible-playbook spark.yml
}
main() {
    check_root
    install_ansible
    verify_installation
    installation_spark
    echo -e "\n${GREEN}Processo de instalação do Ansible concluído!${NC}"
    echo -e "Para começar, edite o arquivo de inventário em /etc/ansible/hosts."
}

main
