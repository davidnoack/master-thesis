package de.noack.resources;

import de.noack.service.CsdbService;
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
 * This resource serves all csdbs of SHSDB retrieved within the last five years. Posted Csdbs will be persisted to a commit log selected within
 * the application properties. One csdb can be retrieved in a transformed way or as raw data. One dataset can only be accessed when it has been
 * transformed to JSON beforehand.
 *
 * @author davidnoack
 */
@Path("/csdbs")
public class CsdbResource {
    private static final String SERVICE_URI = "/csdbs/";
    private static final Logger LOGGER = LoggerFactory.getLogger(CsdbResource.class);

    @Inject
    private CsdbService csdbService;

    @POST
    @Consumes({APPLICATION_OCTET_STREAM, "text/csv"})
    public Response createVanillaCsdb(byte[] content) {
        try {
            final String messageKey = csdbService.produce(content);
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
    public Response getVanillaCsdbs() {
        try {
            final StreamingOutput stream = csdbService::allVanillaCsdbs;
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
    public Response getVanillaCsdb(@PathParam("id") String messageKey) {
        try {
            return ok(csdbService.findVanillaCsdb(messageKey), APPLICATION_OCTET_STREAM).build();
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
    public Response getTransformedCsdbs() {
        try {
            return ok(csdbService.allTransformedCsdbs()).build();
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
    public Response getTransformedCsdb(@PathParam("id") final String messageKey) {
        try {
            return ok(csdbService.findTransformedCsdb(messageKey)).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }
}