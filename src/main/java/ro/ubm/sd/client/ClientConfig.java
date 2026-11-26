package ro.ubm.sd.common;

public class ClientConfig {
    private final String coordinatorAddress;

    public ClientConfig(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }

    public static ClientConfig fromArgs(String[] args) {
        String coordinator = "localhost:7001";

        for (int i = 0; i < args.length; i++) {
            if ("--coordinator".equals(args[i])) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Lipseste valoarea pentru --coordinator");
                }
                coordinator = args[++i];
            } else {
                throw new IllegalArgumentException("Argument necunoscut: " + args[i]);
            }
        }

        if (coordinator.isBlank()) {
            throw new IllegalArgumentException("Adresa coordonatorului nu poate fi goala");
        }

        return new ClientConfig(coordinator);
    }

    public String getCoordinatorAddress() {
        return coordinatorAddress;
    }
}
