package serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    public void configure(Map<String,?> config, boolean iskey){
        System.out.println("序列化config");
    }

    // T data, T 是SpecificRecordBase的子类，getSchema()方法是在其父类中定义的
    public byte[] serialize(String topic, T data){
        System.out.println("开始进行序列化");
        DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);
        try{
            datumWriter.write(data, binaryEncoder);
        }catch (IOException e){
            e.printStackTrace();
        }
        try{
            System.out.println("序列化的结果："+baos.toString("utf-8"));
        }catch (Exception e){}

        return baos.toByteArray();
    }

    public void close(){
        System.out.println("序列化closing");
    }
}
