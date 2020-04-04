package de.noack.resources;

import de.noack.service.ReportService;
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

/**
 * This resource serves all reports of SHSDB retrieved within the last five years. Posted Reports will be persisted to a commit log selected within
 * the application properties. One report can be retrieved in a transformed way or as raw data. One dataset can only be accessed when it has been
 * transformed to JSON beforehand.
 *
 * @author davidnoack
 */
@Path("/reports")
public class ReportResource {
    private static final String SERVICE_URI = "/reports/";
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportResource.class);

    @Inject
    private ReportService reportService;

    @POST
    @Consumes({APPLICATION_OCTET_STREAM, "text/csv"})
    public Response createVanillaReport(byte[] content) {
        try {
            final String messageKey = reportService.produce(content);
            return created(new URI(SERVICE_URI + messageKey)).build();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return serverError()
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Produces(TEXT_PLAIN)
    public Response getVanillaReports() {
        try {
            final StreamingOutput stream = reportService::allVanillaReports;
            return ok(stream).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Path("{id}")
    @Produces(TEXT_PLAIN)
    public Response getVanillaReport(@PathParam("id") String messageKey) {
        try {
            return ok(reportService.findVanillaReport(messageKey), APPLICATION_OCTET_STREAM).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Path("transformed")
    @Produces({APPLICATION_JSON, TEXT_PLAIN})
    public Response getTransformedReports() {
        try {
            return ok(reportService.allTransformedReports()).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Path("transformed/{id}")
    @Produces({APPLICATION_JSON, TEXT_PLAIN})
    public Response getTransformedReport(@PathParam("id") final String messageKey) {
        try {
            return ok(reportService.findTransformedReport(messageKey)).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }
}