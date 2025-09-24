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
    echo -e "\n${YELLOW}Instalando programas...${NC}"
    dnf install -y ansible-core python-pip
    CDRET1=$?
    pip install boto3 botocore
    CDRET2=$?
    ansible-galaxy collection install amazon.aws community.aws
    CDRET3=$?
    if [ $CDRET1 -ne 0 || $CDRET2 -ne 0 || $CDRET3 -ne 0 ]; then
        echo -e "${RED}Falha na instalacao. Verifique os logs do DNF.${NC}"
        exit 1
    fi
    echo -e "${GREEN}PAcotes instalados!${NC}"
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

installation_emr() {
    echo -e "\n${YELLOW}Instalando o Spark...${NC}"
    ansible-playbook emr.yml
}
main() {
    check_root
    install_ansible
    # Se argumento não foi passado, pede
    if [ -z "$1" ]; then
        read -p "Digite o nome da chave PEM cadastrada na AWS: " KEY_NAME
    else
        KEY_NAME=$1
    fi
    installation_emr
    echo -e "\n${GREEN}Processo concluído!${NC}"
}

main "$@"
