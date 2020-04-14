package de.noack.resources;

import de.noack.service.DashboardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.client.Entity.entity;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;

@Path("/dashboard")
public class DashboardResource {
    private static final String SERVICE_URI = "/dashboard/";
    private static final Logger LOGGER = LoggerFactory.getLogger(DashboardResource.class);

    @Inject
    private DashboardService dashboardService;

    @GET
    @Produces({APPLICATION_JSON, TEXT_PLAIN})
    public Response getTransformedReports() {
        try {
            return ok(dashboardService.allMicroData()).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }

    @GET
    @Path("instrumentClassesWithCount")
    @Produces({APPLICATION_JSON, TEXT_PLAIN})
    public Response getReportedInstrumentClassesWithCount() {
        try {
            return ok(dashboardService.instrumentClassesWithCount()).build();
        } catch (RuntimeException e) {
            LOGGER.error(e.getMessage());
            return status(NOT_FOUND)
                    .entity(entity(e.getMessage(), TEXT_PLAIN))
                    .build();
        }
    }
}