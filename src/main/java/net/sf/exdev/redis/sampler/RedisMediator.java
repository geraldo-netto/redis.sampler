package net.sf.exdev.redis.sampler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class RedisMediator {

	private Jedis client = null;
	private static final Logger LOG = LoggerFactory.getLogger(RedisMediator.class);

	public RedisMediator(String host, int port, File file) {
		client = new Jedis(host, port);
		LOG.info("Redis configured: {}:{}  inputfile: {}", host, port, file);
	}

	public long append(String key, String value) {
		return client.append(key, value);
	}

	public String set(String key, String value) {
		return client.set(key, value);
	}

	public String get(String key) {
		return client.get(key);
	}

	public long delete(String key) {
		return client.del(key);
	}

	public long expire(String key, int seconds) {
		return client.expire(key, seconds);
	}

	public boolean flush() {
		client.flushAll();
		return true;
	}

	public List<String> getFileContent(File file) throws IOException {
		List<String> lines = new ArrayList<>();
		try (BufferedReader buffer = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = buffer.readLine()) != null) {
				lines.add(line);
			}
		}

		return lines;
	}

	public Map<String, Object> parse(List<String> lines) {
		int total = 0;
		int error = 0;
		Map<String, Object> result = new HashMap<>();
		List<String> errorMessages = new ArrayList<>();

		for (String line : lines) {
			String[] command = line.trim().split("  ");
			try {
				switch (command[0].toLowerCase()) {
				case "append":
					append(command[1], command[2]);
					break;

				case "set":
					set(command[1], command[2]);
					break;

				case "get":
					get(command[1]);
					break;

				case "delete":
					delete(command[1]);
					break;

				case "expire":
					expire(command[1], Integer.parseInt(command[2]));
					break;

				case "flush":
					flush();
					break;

				default:
					error++;
					errorMessages.add(String.format("Input line %s is malformed", line));
					LOG.error("Input line {} is malformed", line);
				}

			} catch (Exception ex) {
				error++;
				errorMessages.add(String.format("Unable to execute line %s => %s", line, ex.getMessage()));
				LOG.error("Unable to execute line {} => {}", line, ex.getMessage(), ex);
			}

			total++;
		}

		result.put("total", total);
		result.put("success", total - error);
		result.put("errors", error);
		result.put("error-messages", errorMessages);

		return result;
	}

	public boolean close() {
		client.close();
		return true;
	}

}
