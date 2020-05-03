package de.noack.service;

import de.noack.client.DashboardClient;
import de.noack.client.pulsar.DashboardPulsarClient;
import de.noack.model.MicroData;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

@RequestScoped
public class DashboardService {
    private static final String IMPLEMENTATION_MISSING = "Not yet implemented!";
    private final DashboardClient dashboardClient;

    @Inject
    public DashboardService(@ConfigProperty(name = "commitlog") final CommitLog commitLog, final DashboardPulsarClient dashboardPulsarClient) {
        super();
        switch (commitLog) {
            case PULSAR:
                dashboardClient = dashboardPulsarClient;
                break;
            case KAFKA:
                // TODO: Implement
                throw new RuntimeException(IMPLEMENTATION_MISSING);
            default:
                throw new RuntimeException(IMPLEMENTATION_MISSING);
        }
    }

    public Set<MicroData> allMicroData() {
        return dashboardClient.readAllMicroData();
    }

    public Map<String, Long> instrumentClassesWithCount() {
        return dashboardClient.readAllMicroData().stream()
                .map(microData -> microData.getSecurity().getInstrumentClass())
                .filter(string -> string != null && !string.isEmpty())
                .collect(groupingBy(identity(), counting()));
    }
}