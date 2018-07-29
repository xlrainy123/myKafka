package producer;


import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

class Personed implements Serializer<Person> {

    public void close(){

    }

    public void configure(Map configs, boolean iskey){
        System.out.println("hello");
    }

    public byte[] serialize(String topic, Person person){

        if (person == null)
            return null;

        byte[] personName = null;
        int nameLength = 0;

        if (person.getName() != null){
            try {
                personName = person.getName().getBytes("utf-8");
                nameLength = personName.length;
            }catch (UnsupportedEncodingException e){
                e.printStackTrace();
            }
        }else {
            personName = new byte[0];
            nameLength = 0;
        }

        ByteBuffer bb = ByteBuffer.allocate(4+4+nameLength);

        bb.putInt(person.getAge());
        bb.putInt(nameLength);
        bb.put(personName);

        return bb.array();
    }

}
