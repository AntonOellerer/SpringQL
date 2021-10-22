#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_engine::executor::row::Row;

    const REMOTE_PORT: u16 = 17890;

    // TODO JSON to socket

    #[test]
    fn test_input_server_tcp() -> crate::error::Result<()> {
        let model = ServerModel::new(
            ServerType::InputNet,
            OptionBuilder::new()
                .add("PROTOCOL", "TCP")
                .add("REMOTE_HOST", "127.0.0.1")
                .add("REMOTE_PORT", REMOTE_PORT.to_string())
                .build(),
        );

        let server = InputNetServer::new(model)?;
        server.start()?;

        let row_chunk = server.next()?;
        assert_eq!(row_chunk.next(), Some(Row::fx_tokyo()));
        assert_eq!(row_chunk.next(), Some(Row::fx_osaka()));
        assert_eq!(row_chunk.next(), Some(Row::fx_london()));
        assert_eq!(row_chunk.next(), None);

        Ok(())
    }
}
