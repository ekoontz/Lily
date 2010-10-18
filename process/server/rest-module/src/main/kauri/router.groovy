builder.router {
    filter(uri: "", ofClass: "org.lilycms.rest.LocationHeaderFilter") {
        jaxRs() {
            jaxRsResource(scanPackages: ["org.lilycms.rest"])
            jaxRsProvider(scanPackages: ["org.lilycms.rest.providers.json"])
        }
    }
}