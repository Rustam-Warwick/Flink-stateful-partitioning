package StreamPartitioning.serializers;

import StreamPartitioning.features.Feature;
import com.google.protobuf.ByteString;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.statefun.flink.core.generated.Payload;
import org.apache.flink.statefun.flink.core.message.MessagePayloadSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;

public class MyKryo implements MessagePayloadSerializer {

    private final KryoSerializer<Object> kryo = new KryoSerializer<>(Object.class, new ExecutionConfig());
    private final DataInputDeserializer source = new DataInputDeserializer();
    private final DataOutputSerializer target = new DataOutputSerializer(4096);

    @Override
    public Payload serialize(@Nonnull Object payloadObject) {
        target.clear();
        try {
            kryo.serialize(payloadObject, target);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        ByteString serializedBytes = ByteString.copyFrom(target.getSharedBuffer(), 0, target.length());
        return Payload.newBuilder()
                .setClassName(payloadObject.getClass().getName())
                .setPayloadBytes(serializedBytes)
                .build();
    }

    @Override
    public Object deserialize(@Nonnull ClassLoader targetClassLoader, @Nonnull Payload payload) {
        source.setBuffer(payload.getPayloadBytes().asReadOnlyByteBuffer());
        try {
            return kryo.deserialize(source);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Object copy(@Nonnull ClassLoader targetClassLoader, @Nonnull Object what) {
        target.clear();
        try {
            kryo.serialize(what, target);
            source.setBuffer(target.getSharedBuffer(), 0, target.length());

            final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(targetClassLoader);
            try {
                final ClassLoader originalKryoCl = kryo.getKryo().getClassLoader();
                kryo.getKryo().setClassLoader(targetClassLoader);
                try {
                    return kryo.deserialize(source);
                } finally {
                    kryo.getKryo().setClassLoader(originalKryoCl);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

