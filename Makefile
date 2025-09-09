CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2 -g -D_GNU_SOURCE
LDFLAGS = -lrdkafka -lpthread

TARGET = topic-copy
SOURCES = topic-copy.c config.c utils.c mirroring.c message_queue.c cross_cluster.c
OBJECTS = $(SOURCES:.c=.o)
HEADERS = kafka_mirror.h config.h utils.h

.PHONY: all clean install uninstall

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)

install: $(TARGET)
	cp $(TARGET) /usr/local/bin/

uninstall:
	rm -f /usr/local/bin/$(TARGET)

debug: CFLAGS += -DDEBUG -g3
debug: $(TARGET)

release: CFLAGS += -DNDEBUG -O3
release: clean $(TARGET)

test: $(TARGET)
	@echo "Запуск тестов..."
	@echo "Тест 1: Проверка аргументов командной строки"
	./$(TARGET) --help > /dev/null && echo "✓ Тест 1 пройден" || echo "✗ Тест 1 провален"
	@echo "Тест 2: Проверка валидации конфигурации"
	./$(TARGET) --source-brokers localhost:9092 --topics test-topic > /dev/null 2>&1 && echo "✓ Тест 2 пройден" || echo "✗ Тест 2 провален"

format:
	@echo "Форматирование кода..."
	@if command -v clang-format >/dev/null 2>&1; then \
		clang-format -i *.c *.h; \
		echo "Код отформатирован с помощью clang-format"; \
	else \
		echo "clang-format не найден, пропуск форматирования"; \
	fi

lint:
	@echo "Проверка кода..."
	@if command -v cppcheck >/dev/null 2>&1; then \
		cppcheck --enable=all --std=c99 *.c; \
	else \
		echo "cppcheck не найден, пропуск проверки"; \
	fi

deps:
	@echo "Проверка зависимостей..."
	@pkg-config --exists rdkafka && echo "✓ librdkafka найден" || echo "✗ librdkafka не найден"
	@echo "Для установки зависимостей:"
	@echo "  Ubuntu/Debian: sudo apt-get install librdkafka-dev"
	@echo "  Ubuntu/Debian: sudo apt-get install make cmake"
	@echo "  CentOS/RHEL: sudo yum install librdkafka-devel"
	@echo "  macOS: brew install librdkafka"

help:
	@echo "Доступные команды:"
	@echo "  all      - Собрать проект (по умолчанию)"
	@echo "  clean    - Удалить скомпилированные файлы"
	@echo "  install  - Установить в /usr/local/bin"
	@echo "  uninstall- Удалить из /usr/local/bin"
	@echo "  debug    - Собрать с отладочной информацией"
	@echo "  release  - Собрать оптимизированную версию"
	@echo "  test     - Запустить базовые тесты"
	@echo "  format   - Отформатировать код"
	@echo "  lint     - Проверить код"
	@echo "  deps     - Проверить зависимости"
	@echo "  help     - Показать эту справку"
