package simpledb.server

import java.rmi.registry.LocateRegistry

// TODO simpleijでembdeddriverで書き換え
fun main(args: Array<String>) {
    // configure and initialize the database
    val directoryName = if (args.size === 0) {
        "studentdb"
    } else {
        args[0]
    }
    val db = SimpleDB(directoryName)

    // create a registry specific for the server on the default port
    val registry = LocateRegistry.createRegistry(1099)

    // and post the server entry in it
    val d = Rem
}
