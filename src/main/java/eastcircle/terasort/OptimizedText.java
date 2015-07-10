package eastcircle.terasort;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NormalizableKey;
import org.apache.hadoop.io.Text;

public final class OptimizedText implements NormalizableKey<OptimizedText> {

    private final Text text;

    public OptimizedText () {
        this.text = new Text();
    }

    public OptimizedText (Text from) {
        this.text = from;
    }

    public Text getText() {
        return text;
    }

    @Override
    public int getMaxNormalizedKeyLen() {
        return 10;
    }

    @Override
    public void copyNormalizedKey(MemorySegment memory, int offset, int len) {
        memory.put(offset, text.getBytes(), 0, Math.min(text.getLength(), Math.min(10, len)));
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        text.write(out);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        text.readFields(in);
    }

    @Override
    public int compareTo(OptimizedText o) {
        return this.text.compareTo(o.text);
    }
}
