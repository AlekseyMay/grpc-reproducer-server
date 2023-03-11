package com.reproducer;

import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@GrpcService
public class StreamStringGrpcService implements StreamSvc {

    @Override
    public Uni<StringReply> stringStream(Multi<StringRequest> request) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        return request
//                .call(() -> Uni.createFrom().failure(new RuntimeException("Any error")))
                .map(x -> {
                    log.info("Counter: {}", atomicInteger.incrementAndGet());
                    if (atomicInteger.get() == 30) {
                        throw new RuntimeException("We reached 30, error here");
                    }
                    return StringReply.newBuilder()
                            .setMessage(x.toString())
                            .build();
                })
                .collect().asList()
                .replaceWith(StringReply.newBuilder()
                        .setMessage("DONE")
                        .build())
                .onFailure()
                .invoke(th -> log.error(th.getMessage()));
    }

}
