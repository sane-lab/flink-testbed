package stock;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;

public class KafkaWithTsMsgSchema implements DeserializationSchema<Tuple2<String, String>>, SerializationSchema<Tuple2<String, String>> {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public KafkaWithTsMsgSchema() {
        this(Charset.forName("UTF-8"));
    }

    public KafkaWithTsMsgSchema(Charset charset) {
        this.charset = Preconditions.checkNotNull(charset);
    }

    public Charset getCharset() {
        return this.charset;
    }

    // split stock string with delimeter
    public Tuple2<String, String> deserialize(byte[] message) {
        String msg = new String(message, charset);
        String[] stockArr = msg.split("\\|");

        if(stockArr.length > 2){
            return new Tuple2<String, String>(stockArr[Sec_Code], stockArr[Order_Price]);
        }else{
            System.out.println("Fail due to invalid msg format.. ["+msg+"]");
            return new Tuple2<String, String>(msg, "0");
        }
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, String> stringLongTuple2) {
        return false;
    }

    public byte[] serialize(Tuple2<String, String> element) {
        return "MAX - ".concat(element.f0).concat("#").concat(String.valueOf(element.f1)).getBytes(this.charset);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<Tuple2<String, String>>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }}