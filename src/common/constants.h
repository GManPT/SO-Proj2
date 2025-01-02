// constantes partilhadas entre cliente e servidor
#define MAX_SESSION_COUNT 8  // num max de sessoes no server, 1 default, necessario alterar mais tarde
#define STATE_ACCESS_DELAY_US  // delay a aplicar no server
#define MAX_PIPE_PATH_LENGTH 40 // tamanho max do caminho do pipe
#define MAX_STRING_SIZE 40
#define MAX_NUMBER_SUB 10
#define BUFFER_SIZE 122 // tamanho do buffer
#define TEMP_FOLDER "/tmp/" // pasta temporaria
#define MAX_RESPONSE_SIZE 3 // tamanho max da resposta
#define MAX_SIZE_OPCODE 2 // tamanho max do opcode
#define BUFFER_SIZE_ACONNECT 43 // Opcode + 41 bytes + \0
#define MAX_WRITE_SIZE_RESPONSE 301 // MAX_WRITE_SIZE + MAX_STRING_SIZE + , + ( + ) + \0