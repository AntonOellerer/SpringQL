// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Demo application in <https://springql.github.io/get-started/write-basic-apps/#app2-window-aggregation>.
//!
//! Usage:
//!
//! ```bash
//! cargo run --example test_join
//! ```
//!
//! ```bash
//! echo '{"ts": "2022-01-01 13:00:00.000000000", "sensor_id": "ORCL", "reading": 10}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:01.000000000", "sensor_id": "ORCL", "reading": 30}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:01.000000000", "sensor_id": "GOOGL", "reading": 50}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:02.000000000", "sensor_id": "ORCL", "reading": 40}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:05.000000000", "sensor_id": "GOOGL", "reading": 60}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:10.000000000", "sensor_id": "APPL", "reading": 100}' |nc localhost 54300
//! ```

use std::{
    thread,
    time::{Duration, Instant},
};
use std::process::Command;

use springql::{SpringConfig, SpringPipeline};

fn send_data_to_pipeline() {
    fn send_row(sensor_id: u32, json: &str) {
        let cmd_text = format!(r#"echo '{}' |nc localhost 5430{sensor_id}"#, json);
        Command::new("bash")
            .arg("-c")
            .arg(cmd_text)
            .spawn()
            .expect("send failed");
        // yield s.t. the os can execute the command and the order of the messages is upheld
        thread::sleep(Duration::from_millis(50));
    }

    send_row(0, r#"{"ts": "2022-01-01 13:00:00.000000000", "sensor_id": 0, "reading": 10}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:00.000000000", "sensor_id": 1, "reading": 20}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:00.000000000", "sensor_id": 2, "reading": 30}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:00.000000000", "sensor_id": 3, "reading": 40}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:01.000000000", "sensor_id": 0, "reading": 12}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:01.000000000", "sensor_id": 1, "reading": 23}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:01.000000000", "sensor_id": 2, "reading": 34}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:01.000000000", "sensor_id": 3, "reading": 45}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:02.000000000", "sensor_id": 0, "reading": 1}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:02.000000000", "sensor_id": 1, "reading": 2}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:02.000000000", "sensor_id": 2, "reading": 3}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:02.000000000", "sensor_id": 3, "reading": 4}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:03.000000000", "sensor_id": 0, "reading": 1}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:03.000000000", "sensor_id": 1, "reading": 2}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:03.000000000", "sensor_id": 2, "reading": 3}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:03.000000000", "sensor_id": 3, "reading": 4}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:04.000000000", "sensor_id": 0, "reading": 1}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:04.000000000", "sensor_id": 1, "reading": 2}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:04.000000000", "sensor_id": 2, "reading": 3}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:04.000000000", "sensor_id": 3, "reading": 4}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:11.000000000", "sensor_id": 0, "reading": 50}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:11.000000000", "sensor_id": 1, "reading": 60}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:11.000000000", "sensor_id": 2, "reading": 70}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:11.000000000", "sensor_id": 3, "reading": 80}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:21.000000000", "sensor_id": 0, "reading": 11}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:21.000000000", "sensor_id": 1, "reading": 32}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:21.000000000", "sensor_id": 2, "reading": 13}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:21.000000000", "sensor_id": 3, "reading": 34}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:31.000000000", "sensor_id": 0, "reading": 15}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:31.000000000", "sensor_id": 1, "reading": 36}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:31.000000000", "sensor_id": 2, "reading": 17}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:31.000000000", "sensor_id": 3, "reading": 38}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:41.000000000", "sensor_id": 0, "reading": 19}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:41.000000000", "sensor_id": 1, "reading": 10}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:41.000000000", "sensor_id": 2, "reading": 20}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:41.000000000", "sensor_id": 3, "reading": 30}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:00:51.000000000", "sensor_id": 0, "reading": 40}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:00:51.000000000", "sensor_id": 1, "reading": 50}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:00:51.000000000", "sensor_id": 2, "reading": 60}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:00:51.000000000", "sensor_id": 3, "reading": 70}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:01:21.000000000", "sensor_id": 0, "reading": 80}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:01:21.000000000", "sensor_id": 1, "reading": 90}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:01:21.000000000", "sensor_id": 2, "reading": 10}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:01:21.000000000", "sensor_id": 3, "reading": 30}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:01:41.000000000", "sensor_id": 0, "reading": 11}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:01:41.000000000", "sensor_id": 1, "reading": 32}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:01:41.000000000", "sensor_id": 2, "reading": 13}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:01:41.000000000", "sensor_id": 3, "reading": 34}"#);

    send_row(0, r#"{"ts": "2022-01-01 13:01:51.000000000", "sensor_id": 0, "reading": 11}"#);
    send_row(1, r#"{"ts": "2022-01-01 13:01:51.000000000", "sensor_id": 1, "reading": 32}"#);
    send_row(2, r#"{"ts": "2022-01-01 13:01:51.000000000", "sensor_id": 2, "reading": 13}"#);
    send_row(3, r#"{"ts": "2022-01-01 13:01:51.000000000", "sensor_id": 3, "reading": 34}"#);
}

fn main() {
    let mut config = SpringConfig::default();
    config.web_console.enable_report_post = true;
    let pipeline = SpringPipeline::new(&config).unwrap();
    pipeline
        .command(format!(
            "
                CREATE SINK STREAM motor_averages (
                    min_ts TIMESTAMP NOT NULL ROWTIME,
                    air_temperature FLOAT,
                    process_temperature FLOAT,
                    rotational_speed FLOAT,
                    torque FLOAT
                );
                ", ))
        .unwrap();
    for sensor_id in 0..=3 {
        pipeline
            .command(format!(
                "
                    CREATE SOURCE STREAM sensor_data_{sensor_id} (
                        ts TIMESTAMP NOT NULL ROWTIME,
                        sensor_id INTEGER NOT NULL,
                        reading FLOAT NOT NULL
                    );
                    ", ))
            .unwrap();

        pipeline
            .command(format!(
                "
                    CREATE SOURCE READER sensor_data_reader_{sensor_id} FOR sensor_data_{sensor_id}
                        TYPE NET_SERVER OPTIONS (
                        PROTOCOL 'TCP',
                        PORT '5430{sensor_id}'
                    );
                    "))
            .unwrap();

        pipeline
            .command(format!(
                "
                    CREATE STREAM sensor_average_{sensor_id} (
                        min_ts TIMESTAMP NOT NULL ROWTIME,
                        sensor_id INTEGER NOT NULL,
                        avg_reading FLOAT NOT NULL
                    );
                    "
            ))
            .unwrap();

        pipeline
            .command(format!(
                "
                    CREATE PUMP pump_sensor_average_{sensor_id} AS
                    INSERT INTO sensor_average_{sensor_id} (min_ts, sensor_id, avg_reading)
                    SELECT STREAM
                        FLOOR_TIME(sensor_data_{sensor_id}.ts, DURATION_SECS(10)) AS min_ts,
                        sensor_data_{sensor_id}.sensor_id AS sensor_id,
                        AVG(sensor_data_{sensor_id}.reading) AS avg_reading
                    FROM sensor_data_{sensor_id}
                    GROUP BY min_ts, sensor_id
                    FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
                    "
            ))
            .unwrap()
    }


    pipeline
        .command(format!(
            "CREATE STREAM sensor_data_joined_0_1 (
                    min_ts TIMESTAMP NOT NULL ROWTIME,
                    air_temperature FLOAT,
                    process_temperature FLOAT
                )"
        ))
        .unwrap();

    pipeline
        .command(format!(
            "
                CREATE PUMP sensor_join_values_0_1 AS
                    INSERT INTO sensor_data_joined_0_1 (min_ts, air_temperature, process_temperature)
                    SELECT STREAM
                        sensor_average_0.min_ts,
                        sensor_average_0.avg_reading,
                        sensor_average_1.avg_reading
                    FROM sensor_average_0
                    LEFT OUTER JOIN sensor_average_1
                        ON sensor_average_0.min_ts = sensor_average_1.min_ts
                    FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
                ", ))
        .unwrap();

    pipeline
        .command(format!(
            "CREATE STREAM sensor_data_joined_2_3 (
                    min_ts TIMESTAMP NOT NULL ROWTIME,
                    rotational_speed FLOAT,
                    torque FLOAT
                )"
        ))
        .unwrap();

    pipeline
        .command(format!(
            "
                CREATE PUMP sensor_join_values_2_3 AS
                    INSERT INTO sensor_data_joined_2_3 (min_ts, rotational_speed, torque)
                    SELECT STREAM
                        sensor_average_2.min_ts,
                        sensor_average_2.avg_reading,
                        sensor_average_3.avg_reading
                    FROM sensor_average_2
                    LEFT OUTER JOIN sensor_average_3
                        ON sensor_average_2.min_ts = sensor_average_3.min_ts
                    FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
                ", ))
        .unwrap();

    pipeline
        .command(format!(
            "
                CREATE PUMP window_avg_values AS
                    INSERT INTO motor_averages (min_ts, air_temperature, process_temperature, rotational_speed, torque)
                    SELECT STREAM
                        sensor_data_joined_0_1.min_ts,
                        sensor_data_joined_0_1.air_temperature,
                        sensor_data_joined_0_1.process_temperature,
                        sensor_data_joined_2_3.rotational_speed,
                        sensor_data_joined_2_3.torque
                    FROM sensor_data_joined_0_1
                    LEFT OUTER JOIN sensor_data_joined_2_3
                        ON sensor_data_joined_0_1.min_ts = sensor_data_joined_2_3.min_ts
                    FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
                ", ))
        .unwrap();

    pipeline
        .command(format!(
            "
                CREATE SINK WRITER queue_writer FOR motor_averages
                TYPE IN_MEMORY_QUEUE OPTIONS (
                    NAME 'motor_averages'
                );
            ", ))
        .unwrap();

    eprintln!("created pipeline, sending messages");
    send_data_to_pipeline();
    eprintln!("Sent messages, fetching results");

    let start_at = Instant::now();
    loop {
        if let Ok(Some(row)) = pipeline.pop_non_blocking("motor_averages") {
            let ts: String = row.get_not_null_by_index(0).unwrap();
            let air_temperature: f32 = row.get_not_null_by_index(1).unwrap_or(-1f32);
            let process_temperature: f32 = row.get_not_null_by_index(2).unwrap_or(-1f32);
            let rotational_speed: f32 = row.get_not_null_by_index(3).unwrap_or(-1f32);
            let torque: f32 = row.get_not_null_by_index(4).unwrap_or(-1f32);
            eprintln!("[motor_average_readings]\t{ts}\t{air_temperature}\t{process_temperature}\t{rotational_speed}\t{torque}");
        }

        // Avoid busy loop
        thread::sleep(Duration::from_millis(10));
        // exit with 5 second
        if Instant::now() - start_at > Duration::from_secs(60) {
            return;
        }
    }
}
