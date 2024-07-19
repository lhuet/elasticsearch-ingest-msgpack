package org.elasticsearch.plugin.ingest.msgpack;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

public class MsgpackProcessor extends AbstractProcessor {

    public static final String TYPE = "msgpack";
    private final String input;
    private final String target;
    private final String inputFormat;

    protected MsgpackProcessor(String tag, String description, String input, String target, String inputFormat) {
        super(tag, description);
        this.input = input;
        this.target = target;
        this.inputFormat = inputFormat;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {

        // As the plugin support String and String Array automatically,
        // we guess what type is the input (String or ArrayList)
        String inputType = ingestDocument.getFieldValue(input, Object.class).getClass().getSimpleName();

        switch (inputType) {
            case "String":
                String encodedMessage = ingestDocument.getFieldValue(input, String.class);
                ingestDocument.setFieldValue(target, fromMsgpackToObject(encodedMessage, inputFormat));
                break;

            case "ArrayList":
                ArrayList<String> encodedMessageList = ingestDocument.getFieldValue(input, ArrayList.class);
                List<Object> targetResult = new ArrayList<>();
                for (String encodedMsg : encodedMessageList) {
                    targetResult.add(fromMsgpackToObject(encodedMsg, inputFormat));
                }
                ingestDocument.setFieldValue(target, targetResult);
                break;
        }

        return super.execute(ingestDocument);
    }

    private Object fromMsgpackToObject(String encodedMessage, String inputFormat) throws IOException {

        byte[] msgpackMessage = switch (inputFormat) {
            case "hex" -> HexFormat.of().parseHex(encodedMessage);
            case "base64" -> Base64.getDecoder().decode(encodedMessage);
            default -> Base64.getDecoder().decode(encodedMessage);
        };

        // Msgpack library use Reflection that requires specific permission
        // See plugin-security.policy file
        byte[] finalMsgpackMessage = msgpackMessage;
        List<String> decodedResult = AccessController.doPrivileged((PrivilegedAction<List<String>>) () -> {
            try {
                return decodeMsgpack(finalMsgpackMessage);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        if (decodedResult.size() == 1) {
            return fromStringToJson(decodedResult.get(0));
        } else {
            List<Object> targetResult = new ArrayList<>();
            for (String item: decodedResult) {
                targetResult.add(fromStringToJson(item));
            }
            return targetResult;
        }

    }

    private static Object fromStringToJson(String jsonInputString) throws IOException {

        // Code taken from Elasticsearch ingest common JSONProcessor class
        // TODO: try to find a way to import the ingest-common module in Gradle build file to avoid copy/paste this code

        XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                jsonInputString
        );
        XContentParser.Token token = parser.nextToken();
        Object value = null;
        if (token == XContentParser.Token.VALUE_NULL) {
            value = null;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            value = parser.text();
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            value = parser.numberValue();
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            value = parser.booleanValue();
        } else if (token == XContentParser.Token.START_OBJECT) {
            value = parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            value = parser.list();
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            throw new IllegalArgumentException("cannot read binary value");
        }

        return value;
    }

    private List<String> decodeMsgpack(byte[] encodedData) throws Exception{

        List<String> result = new ArrayList<>();

        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(encodedData);
        while (unpacker.hasNext()) {
            Value v = unpacker.unpackValue();
            switch (v.getValueType()) {
                case NIL:
                    result.add("null");
                    break;
                case BOOLEAN:
                    result.add(v.asBooleanValue().toString());
                    break;
                case INTEGER:
                    result.add(v.asIntegerValue().toString());
                    break;
                case FLOAT:
                    result.add(v.asFloatValue().toString());
                    break;
                case STRING:
                    result.add(v.asStringValue().asString());
                    break;
                case BINARY:
                    byte[] mb = v.asBinaryValue().asByteArray();
                    result.add(HexFormat.of().formatHex(mb));
                    break;
                case ARRAY:
                    result.add(v.asArrayValue().toJson());
                    break;
                case MAP:
                    result.add(v.asMapValue().toJson());
                    break;
                case EXTENSION:
                    ExtensionValue ev = v.asExtensionValue();
                    if (ev.isTimestampValue()) {
                        // Reading the value as a timestamp
                        result.add(String.valueOf(ev.asTimestampValue().toEpochMillis()));
                    }
                    /* We ignore the other cases for now
                    else {
                        byte extType = ev.getType();
                        byte[] extValue = ev.getData();
                    }
                    */
                    break;
            }

        }
        return result;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String getInput() {
        return input;
    }

    public String getTarget() {
        return target;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories, String tag, String description, Map<String, Object> config) throws Exception {
            // Input field is mandatory
            String input = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            // Target is optional. If not set, we put the hash value as the document Id
            String target = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "target_field");
            if (target == null) target = "msgpack_decoded";
            // Input format is optional. Use base64 by default
            String inputFormat = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "input_format");
            if (inputFormat == null) {
                inputFormat = EncodedFormat.BASE64.getFormat();
            }
            else {
                inputFormat = EncodedFormat.fromString(inputFormat).getFormat();
            }
            return new MsgpackProcessor(tag, description, input, target, inputFormat);
        }
    }

    enum EncodedFormat {
        HEX("hex"),
        BASE64("base64");

        private String format;

        EncodedFormat(String format) {
            this.format = format;
        }

        private String getFormat() {
            return format;
        }

        private static EncodedFormat fromString(String format) {
            try {
                return EncodedFormat.valueOf(format.toUpperCase());
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Encoded format '" + format + "` not supported");
            }
        }
    }

}
