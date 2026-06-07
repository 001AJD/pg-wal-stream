package main

import (
	"context"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func main() {
	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, "host=localhost port=5432 user=replicator password=secret dbname=domains replication=database")

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	err = conn.Ping(ctx)

	if err != nil {
		log.Fatal(err)
	}

	log.Println("connected to postgres")
	log.Println("Start replication")

	logreplOptions := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names 'pub_domain_cdc'",
		},
	}
	parsedLSN, err := pglogrepl.ParseLSN("0/D7D7A90")

	if err != nil {
		log.Fatal(err)
	}

	pglogreplErr := pglogrepl.StartReplication(ctx, conn, "slot_domain_cdc", parsedLSN, logreplOptions)

	if pglogreplErr != nil {
		log.Fatal(pglogreplErr)
	}

	relations := make(map[uint32]*pglogrepl.RelationMessage)

	for {
		msg, err := conn.ReceiveMessage(ctx)

		if err != nil {
			log.Fatal(err)
		}

		switch msg := msg.(type) {

		case *pgproto3.CopyData:

			switch msg.Data[0] {

			case pglogrepl.PrimaryKeepaliveMessageByteID:
				log.Println("keepalive")

			case pglogrepl.XLogDataByteID:

				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])

				if err != nil {
					log.Fatal("Failed to parse the xlogdatabyteid, exiting...")
				}

				log.Printf("\nLSN: %s\n", xld.WALStart)

				logicalMsg, err := pglogrepl.Parse(xld.WALData)

				if err != nil {
					log.Fatal(err)
				}

				switch logicalMsg := logicalMsg.(type) {

				case *pglogrepl.RelationMessage:

					relations[logicalMsg.RelationID] = logicalMsg

					log.Printf(
						"RELATION: %s.%s\n",
						logicalMsg.Namespace,
						logicalMsg.RelationName,
					)

				case *pglogrepl.InsertMessage:

					relation := relations[logicalMsg.RelationID]

					log.Printf(
						"INSERT INTO %s.%s\n",
						relation.Namespace,
						relation.RelationName,
					)

					for i, col := range logicalMsg.Tuple.Columns {

						colName := relation.Columns[i].Name

						switch col.DataType {

						case 't':
							log.Printf(
								"%s = %s\n",
								colName,
								string(col.Data),
							)

						case 'n':
							log.Printf(
								"%s = NULL\n",
								colName,
							)
						}
					}
				case *pglogrepl.UpdateMessage:

					relation := relations[logicalMsg.RelationID]

					log.Printf(
						"UPDATE %s.%s\n",
						relation.Namespace,
						relation.RelationName,
					)

					for i, col := range logicalMsg.NewTuple.Columns {

						colName := relation.Columns[i].Name

						if col.DataType == 't' {
							log.Printf("%s = %s\n", colName, string(col.Data))
						}
					}

				case *pglogrepl.DeleteMessage:

					relation := relations[logicalMsg.RelationID]

					log.Printf(
						"DELETE FROM %s.%s\n",
						relation.Namespace,
						relation.RelationName,
					)

					for i, col := range logicalMsg.OldTuple.Columns {

						colName := relation.Columns[i].Name

						if col.DataType == 't' {
							log.Printf("%s = %s\n", colName, string(col.Data))
						}
					}

				case *pglogrepl.CommitMessage:

					log.Printf(
						"COMMIT LSN=%s\n",
						logicalMsg.CommitLSN,
					)

				}

			}

		}
	}
}
