package de.noack.resources;

import de.noack.model.CSDB;
import de.noack.service.CsdbService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.Set;

import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.*;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.*;

/**
 * This resource serves all Csdbs of SHSDB retrieved within the last five years. Posted CSDB Periods will be persisted to a commit log selected within
 * the application properties.
 *
 * @author davidnoack
 */
@Path("/CSDB")
public class CsdbResource {
    private static final String SERVICE_URI = "/CSDB/";
    private static final Logger LOGGER = LoggerFactory.getLogger(CsdbResource.class);

    @Inject
    CsdbService csdbService;

    @POST
    @Consumes({APPLICATION_OCTET_STREAM, "text/csv"})
    @Produces(APPLICATION_JSON)
    public Response createCsdb(byte[] content) {
        try {
            String messageKey = csdbService.produce(content);
            return created(new URI(SERVICE_URI + messageKey)).build();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return serverError()
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Path("{id}")
    @Produces(APPLICATION_JSON)
    public Response getCsdb(@PathParam("id") String messageKey) {
        try {
            return ok(csdbService.read(messageKey), APPLICATION_OCTET_STREAM).build();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            return serverError()
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Produces({APPLICATION_JSON, TEXT_PLAIN})
    public Response getCsdbs() {
        try {
            Set<CSDB> csdbs = csdbService.consume();
            return ok(csdbs).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }
}