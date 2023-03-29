## Código em python para verificação de status do dispositivo

Esse código em python verifica, através de uma thread, se o dispositivo envia sinal a cada 1 minuto, caso ele não envie é inserido no banco um status de "OFFLINE" com data e horário. Ele só insere no banco novamente o status "OFFLINE" caso o dispositivo mande algum sinal, para não mandar "OFFLINE" a cada 1 minuto para o banco, ele envia somente a primeira vez de indisponibilidade.

O código SimulaEnvioLive.py serve para testes, simulando um dispositivo real enviando mensgens ao broker a cada 1 minuto.