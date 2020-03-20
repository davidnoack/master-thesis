package de.noack.resources;

import de.noack.service.ReportService;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.net.URI;

import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.*;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.*;

@Path("/reports")
public class ReportResource {
    private static final String SERVICE_URI = "/reports/";
    private static final Logger log = LoggerFactory.getLogger(ReportResource.class);

    @Inject
    private ReportService reportService;

    @POST
    @Consumes({APPLICATION_OCTET_STREAM, "text/csv"})
    @Produces(APPLICATION_JSON)
    public Response createReport(byte[] content) {
        try {
            String messageKey = reportService.produce(content);
            return created(new URI(SERVICE_URI + messageKey)).build();
        } catch (Exception e) {
            log.error(e.getMessage());
            return serverError()
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Path("{id}")
    @Produces({APPLICATION_OCTET_STREAM, TEXT_PLAIN})
    public Response getReport(@PathParam("id") String messageKey) {
        try {
            return ok(reportService.read(messageKey), APPLICATION_OCTET_STREAM).build();
        } catch (PulsarClientException e) {
            log.error(e.getMessage());
            return serverError()
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Produces({APPLICATION_OCTET_STREAM, TEXT_PLAIN})
    public Response getReports() {
        try {
            StreamingOutput stream = reportService::consume;
            return ok(stream).build();
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }
}