package es.afm.hadoop.examples.inputformat;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;

public class SentenceReader implements Closeable {
	private static final int DEFAULT_BUFFER_SIZE = 4096;

	private byte[] buffer;
	private int bufferLen = 0; // number of bytes in the buffer
	private int bufferPos = 0; // current position
	private InputStream in;

	public SentenceReader(InputStream in) {
		this(in, DEFAULT_BUFFER_SIZE);
	}

	public SentenceReader(InputStream in, int bufferSize) {
		this.in = in;
		this.buffer = new byte[bufferSize];
	}

	public int readSentence(Text str) throws IOException {
		str.clear();
		int puntMarkLen = 0; // length of the end of sentences
		long bytesConsumed = 0;
		do {
			int startPosn = bufferPos; // starting from where we left off the
										// last time
			if (bufferPos >= bufferLen) {
				startPosn = bufferPos = 0;
				bufferLen = fillBuffer(in, buffer);
				if (bufferLen <= 0) {
					break; // EOF
				}
			}
			for (; bufferPos < bufferLen; ++bufferPos) { // search for
															// endOfsentence
				if (buffer[bufferPos] == '.' || buffer[bufferPos] == '!'
						|| buffer[bufferPos] == '?') {
					puntMarkLen = 1;
					++bufferPos;
					break;
				}
			}
			int readLength = bufferPos - startPosn;
			bytesConsumed += readLength;
			int appendLength = readLength - puntMarkLen;
			if (appendLength > 0) {
				str.append(buffer, startPosn, appendLength);
			}
		} while (puntMarkLen == 0);

		if (bytesConsumed > Integer.MAX_VALUE) {
			throw new IOException("Too many bytes before newline: "
					+ bytesConsumed);
		}
		return (int) bytesConsumed;
	}

	protected int fillBuffer(InputStream in, byte[] buffer) throws IOException {
		return in.read(buffer);
	}

	public void close() throws IOException {
		in.close();
	}
}
