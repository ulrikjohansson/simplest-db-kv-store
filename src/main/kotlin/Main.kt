package io.ulrik.db

import kotlinx.cli.*
import kotlin.io.path.Path

@OptIn(ExperimentalCli::class)
fun main(args: Array<String>) {
    val parser = ArgParser("db")
    val storePath by parser.option(ArgType.String, "file", "f", "db file")
    val debug by parser.option(ArgType.Boolean, "debug")
    val dbKey by parser.argument(ArgType.String)

    class Get : Subcommand("get", "Get value for key") {
        val db = Db(FileStoreWithHashMapIndex(Path(storePath ?: "simple.db")))
        var result: String = ""

        override fun execute() {
            result = db.get(dbKey) as String
            println("$result")
            kotlin.system.exitProcess(0)
        }
    }

    class Put : Subcommand("put", "Set value for key") {
        val db = Db(FileStoreWithHashMapIndex(Path(storePath ?: "simple.db")))
        val dbValue by argument(ArgType.String)
        var result: String = ""

        override fun execute() {
            if (debug == true) {
                System.err.println("Debug: key=$dbKey val=$dbValue")
            }
            result = db.put(dbKey, dbValue).toString()
            kotlin.system.exitProcess(0)
        }
    }

    val getCommand = Get()
    val putCommand = Put()
    parser.subcommands(getCommand, putCommand)

    parser.parse(args)
}
