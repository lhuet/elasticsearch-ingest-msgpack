package org.elasticsearch.plugin.ingest.msgpack;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

public class IngestMsgPackPlugin extends Plugin implements IngestPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(MsgpackProcessor.TYPE, new MsgpackProcessor.Factory());
    }

}
