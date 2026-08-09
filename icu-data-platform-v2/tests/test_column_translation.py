from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from asic_pipeline.contracts import load_input_contract
from asic_pipeline.errors import ConfigurationError
from asic_pipeline.translations import (
    MERGE_POLICY_BINARY_OR,
    load_translation_config,
    load_translation_registry,
    translate_and_write_pooled_data,
)


PROJECT_ROOT = Path(__file__).parents[1]
CONTRACT_PATH = (
    PROJECT_ROOT / "asic" / "config" / "pooled" / "contract.yaml"
)
REGISTRY_PATH = (
    PROJECT_ROOT
    / "asic"
    / "config"
    / "pooled_to_translated"
    / "policy.yaml"
)
HOSPITALS = (0, 2, 3, 4, 6, 7, 8)


def _arrow_type(type_name: str) -> pa.DataType:
    return {
        "large_string": pa.large_string(),
        "string": pa.string(),
        "int8": pa.int8(),
        "int32": pa.int32(),
        "float": pa.float32(),
        "double": pa.float64(),
        "timestamp[us]": pa.timestamp("us"),
    }[type_name]


def _dynamic_values(column: str, stay_ids: list[str]) -> list[object]:
    hospitals = [int(stay_id.rsplit(":", 1)[1]) for stay_id in stay_ids]
    if column == "Pseudo-ID":
        return stay_ids
    if column == "hid":
        return hospitals
    if column == "Zeit_ab_Aufnahme":
        return [0.0 if index % 2 == 0 else 60.0 for index in range(len(stay_ids))]
    if column == "timeidx":
        anchor = datetime(2020, 1, 1)
        return [
            anchor + timedelta(minutes=0 if index % 2 == 0 else 60)
            for index in range(len(stay_ids))
        ]
    if column == "ARDS_Diagnose_App_duplicated_0":
        return [None] * len(stay_ids)
    if column in {"CK", "IL-6", "NT-proBNP"}:
        return [
            None if hospital == 2 else float(index + 10)
            for index, hospital in enumerate(hospitals)
        ]
    if column in {"Creatinkinase", "Interleukin_6", "NT-pro_BNP"}:
        return [
            float(index + 10) if hospital == 2 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column in {"ECMO_FiO2", "Lymphocyten"}:
        return [
            float(index + 30) if hospital == 2 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column in {"Gaszusammensetzung_(%O2)", "Lymphozyten_prozentual"}:
        return [
            None if hospital == 2 else float(index + 30)
            for index, hospital in enumerate(hospitals)
        ]
    if column == "pH_Wert_(ohne_Temp-Korrektur)_arteriell":
        return [
            float(index + 40) if hospital == 0 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column == "pH_arteriell":
        return [
            None if hospital == 0 else float(index + 40)
            for index, hospital in enumerate(hospitals)
        ]
    if column == "LesebestaetigugnTherapie_utc":
        return [
            float(index % 2) if hospital != 2 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column == "LesebestaetigungTherapie_utc":
        return [
            float(index % 2) if hospital == 2 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column == "Koerperkerntemperatur":
        return [
            float(index + 20) if hospital != 2 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column == "Körpertemperatur":
        return [
            float(index + 20) if hospital == 2 else None
            for index, hospital in enumerate(hospitals)
        ]
    if column == "ARDS_Diagnose_App":
        return [index % 2 for index in range(len(stay_ids))]
    if column == "ECMO":
        return [float(index % 2) for index in range(len(stay_ids))]
    if column == "Lagerungstherapie":
        approved_values = (0.0, 1.0, 0.5, 0.0)
        return [approved_values[index % 4] for index in range(len(stay_ids))]
    if column == "LesebestaetigungSchweregrad":
        return [None] * len(stay_ids)
    return [float(index + 1) for index in range(len(stay_ids))]


def _static_categorical_values(column: str) -> list[object] | None:
    return {
        "clusterGeschlecht": ["M", "W", "m", " w ", "M", "W", "M"],
        "clusterKoerpergewicht": [
            "<65",
            "65-75",
            "76-250",
            "-1",
            "<65",
            "65-75",
            "76-250",
        ],
        "clusterKoerpergroesse": [
            "<180",
            "180-185",
            ">185",
            "<180",
            "180-185",
            ">185",
            "<180",
        ],
        "BMI": ["L", "M", "P", "1", "2", "3", "X"],
        "Wiederaufnahme_ICU": [0.0, 1.0, None, 0.0, 0.0, 1.0, 0.0],
        "clusterAlter": [
            "<70",
            "70-79",
            "80-130",
            "<70",
            "70-79",
            "80-130",
            "<70",
        ],
        "Entlassgrund_(verlegt_intern,_verlegt_extern,_verstorben)": [
            "verlegt",
            "verstorben",
            "Verlegt",
            "Verstorben",
            " VERLEGT ",
            "verstorben",
            "verlegt",
        ],
        "Sterblichkeit": ["0", "ICU", "KH", "nan", None, "0", "icu"],
        "KH-Sterblichkeit": [
            "false",
            "true",
            None,
            " FALSE ",
            "TRUE",
            None,
            "false",
        ],
        "Liegedauer_KH": [-1.0, -2.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        "Dialyse_(dialysefreie_Tage)": [-1.0, -2.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        "Beatmungsfreie_Tage": [0.0, -1.0, -2.0, 2.0, 3.0, 4.0, 5.0],
    }.get(column)


def _write_pooled_fixture(root: Path) -> tuple[Path, list[str]]:
    contract = load_input_contract(CONTRACT_PATH)
    pooled = root / "asic" / "data" / "demo" / "pooled"
    pooled.mkdir(parents=True)
    static_ids = [f"{1000 + hospital}:{hospital}" for hospital in HOSPITALS]

    static_arrays: list[pa.Array] = []
    static_fields: list[pa.Field] = []
    for column in contract.static.columns:
        arrow_type = _arrow_type(column.arrow_type_for("demo"))
        static_fields.append(pa.field(column.name, arrow_type))
        if column.name == "Pseudo-ID":
            values: list[object] = static_ids
        elif column.name == "hid":
            values = [float(hospital) for hospital in HOSPITALS]
        elif (categorical_values := _static_categorical_values(column.name)) is not None:
            values = categorical_values
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            values = [f"{column.name}_{hospital}" for hospital in HOSPITALS]
        elif pa.types.is_integer(arrow_type):
            values = [index % 3 for index in range(len(HOSPITALS))]
        else:
            values = [float(index + 1) for index in range(len(HOSPITALS))]
        static_arrays.append(pa.array(values, type=arrow_type))
    pq.write_table(
        pa.Table.from_arrays(static_arrays, schema=pa.schema(static_fields)),
        pooled / contract.static.filename,
    )

    dynamic_ids = [stay_id for stay_id in static_ids for _ in range(2)]
    dynamic_arrays: list[pa.Array] = []
    dynamic_fields: list[pa.Field] = []
    for column in contract.dynamic.columns:
        arrow_type = _arrow_type(column.arrow_type_for("demo"))
        dynamic_fields.append(pa.field(column.name, arrow_type))
        dynamic_arrays.append(
            pa.array(_dynamic_values(column.name, dynamic_ids), type=arrow_type)
        )
    pq.write_table(
        pa.Table.from_arrays(dynamic_arrays, schema=pa.schema(dynamic_fields)),
        pooled / contract.dynamic.filename,
        row_group_size=5,
    )
    return pooled, dynamic_ids


def _write_config(root: Path, pooled: Path) -> Path:
    config_path = root / "asic" / "config" / "datasets" / "demo.yaml"
    output = root / "asic" / "data" / "demo" / "translated"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "dataset_context": "demo",
                "contract": str(CONTRACT_PATH),
                "translation_policy": str(REGISTRY_PATH),
                "paths": {
                    "pooled": str(pooled),
                    "translated": str(output),
                },
                "pooled_to_translated": {"batch_size": 4},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path


def test_translate_columns_merges_approved_aliases_and_preserves_rows(
    tmp_path: Path,
) -> None:
    pooled, dynamic_ids = _write_pooled_fixture(tmp_path)
    config = load_translation_config(_write_config(tmp_path, pooled))
    result = translate_and_write_pooled_data(config, reporter=None)

    assert result.audit_status == "pass"
    assert result.static_row_count == 7
    assert result.dynamic_row_count == 14
    assert result.static_column_count == 22
    assert result.dynamic_column_count == 132

    contract = load_input_contract(CONTRACT_PATH)
    registry = load_translation_registry(REGISTRY_PATH, contract)
    source_static = pq.read_table(pooled / contract.static.filename)
    source_dynamic = pq.read_table(pooled / contract.dynamic.filename)
    translated_static = pq.read_table(result.static_path)
    translated_dynamic = pq.read_table(result.dynamic_path)

    assert translated_static.column_names == list(
        registry.output_order_for("static")
    )
    assert translated_dynamic.column_names == list(
        registry.output_order_for("dynamic")
    )

    assert translated_static.column("stay_id_global").to_pylist() == (
        source_static.column("Pseudo-ID").to_pylist()
    )
    assert translated_dynamic.column("stay_id_global").to_pylist() == dynamic_ids
    assert translated_dynamic.column("minutes_since_icu_admission").equals(
        source_dynamic.column("Zeit_ab_Aufnahme")
    )
    assert translated_dynamic.column("anchored_time_since_icu_admission").equals(
        source_dynamic.column("timeidx")
    )
    assert translated_static.column("hospital_id").to_pylist() == [
        f"asic_UK{hospital:02d}" for hospital in HOSPITALS
    ]
    assert translated_static.column("sex").to_pylist() == [
        "male",
        "female",
        "male",
        "female",
        "male",
        "female",
        "male",
    ]
    assert translated_static.column("weight_group").to_pylist() == [
        "<65",
        "65-75",
        "76-250",
        None,
        "<65",
        "65-75",
        "76-250",
    ]
    assert translated_static.column("hosp_los").to_pylist() == [
        None,
        -2.0,
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
    ]
    assert translated_static.column("dialysis_free_days").to_pylist() == [
        None,
        -2.0,
        1.0,
        2.0,
        3.0,
        4.0,
        5.0,
    ]
    assert translated_static.column("vent_free_days").to_pylist() == [
        0.0,
        None,
        -2.0,
        2.0,
        3.0,
        4.0,
        5.0,
    ]
    assert translated_static.column("bmi_group").to_pylist() == [
        "underweight",
        "normal_weight",
        "overweight",
        "obesity_class_1",
        "obesity_class_2",
        "obesity_class_3",
        None,
    ]
    assert translated_static.column("discharge_status").to_pylist() == [
        "transferred",
        "died",
        "transferred",
        "died",
        "transferred",
        "died",
        "transferred",
    ]
    assert translated_static.column("death_status").to_pylist() == [
        "discharged_alive",
        "died_in_icu",
        "died_in_hospital",
        None,
        None,
        "discharged_alive",
        "died_in_icu",
    ]
    assert translated_static.column("hospital_mortality_reported").type == pa.bool_()
    assert translated_static.column("hospital_mortality_reported").to_pylist() == [
        False,
        True,
        None,
        False,
        True,
        None,
        False,
    ]
    for source, target in (
        ("Phase", "study_implementation_phase"),
        ("clusterKoerpergroesse", "height_group"),
        ("Wiederaufnahme_ICU", "icu_readmit"),
        ("clusterAlter", "age_group"),
    ):
        assert translated_static.column(target).equals(source_static.column(source))
    for source, target in (
        ("ARDS_Diagnose_App", "ards_diagnosis_app"),
        ("ECMO", "ecmo"),
        ("Lagerungstherapie", "position_therapy"),
        ("LesebestaetigungSchweregrad", "severity_read_confirmation"),
    ):
        assert translated_dynamic.column(target).equals(
            source_dynamic.column(source)
        )

    names = set(translated_dynamic.column_names)
    assert "ARDS_Diagnose_App_duplicated_0" not in names
    for target in (
        "ck",
        "ecmo_o2",
        "il6",
        "lymph_pct",
        "ntprobnp",
        "ph_art",
        "core_temp",
    ):
        assert target in names
    assert "therapy_read_confirmation_utc" in names
    assert "delta_p_computed" in names
    assert "delta_p_reported" in names
    assert len(translated_dynamic.column_names) == len(names)

    source_vt_per_ideal = source_dynamic.column(
        "individuelles_Tidalvolumen_pro_kg_idealem_Koerpergewicht"
    ).to_pylist()
    translated_base = translated_dynamic.column(
        "vt_per_kg_ideal_body_weight"
    ).to_pylist()
    translated_uk00 = translated_dynamic.column(
        "vt_per_ideal_bw_total"
    ).to_pylist()
    hospitals = source_dynamic.column("hid").to_pylist()
    assert translated_base == [
        None if hospital == 0 else value
        for hospital, value in zip(hospitals, source_vt_per_ideal, strict=True)
    ]
    assert translated_uk00 == [
        value if hospital == 0 else None
        for hospital, value in zip(hospitals, source_vt_per_ideal, strict=True)
    ]

    for rule in registry.merge_rules:
        expected: list[object] = []
        source_columns = [
            source_dynamic.column(source).to_pylist()
            for source in rule.source_columns
        ]
        for values in zip(*source_columns, strict=True):
            non_missing = [value for value in values if value is not None]
            if rule.conflict_policy == MERGE_POLICY_BINARY_OR:
                expected.append(max(non_missing) if non_missing else None)
            else:
                expected.append(non_missing[0] if non_missing else None)
        assert translated_dynamic.column(rule.target_name).to_pylist() == expected

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["audit_status"] == "pass"
    assert (
        manifest["documentation"]
        == "asic/docs/pooled_to_translated.md"
    )
    assert manifest["checks"]["row_counts_preserved"] is True
    assert (
        manifest["checks"]["translated_column_order_matches_registry"] is True
    )
    assert (
        manifest["checks"]["approved_merge_conflict_policies_applied"] is True
    )
    assert manifest["checks"]["categorical_values_recoded"] is True
    assert (
        manifest["checks"]["approved_categorical_value_mappings_applied"]
        is True
    )
    assert (
        manifest["checks"][
            "unmapped_non_missing_values_in_approved_mappings"
        ]
        == 0
    )
    assert manifest["approved_drops"][
        "dynamic.ARDS_Diagnose_App_duplicated_0"
    ]["non_missing_rows"] == 0
    assert set(manifest["approved_merges"]) == {
        rule.name for rule in registry.merge_rules
    }
    assert set(manifest["approved_categorical_value_mappings"]) == {
        "static.sex",
        "static.weight_group",
        "static.bmi_group",
        "static.discharge_status",
        "static.death_status",
        "static.hospital_mortality_reported",
    }
    assert manifest["approved_categorical_value_mappings"][
        "static.weight_group"
    ]["mapped_to_missing"] == 1
    assert manifest["approved_categorical_value_mappings"][
        "static.bmi_group"
    ]["mapped_to_missing"] == 1
    assert set(manifest["approved_numeric_missing_sentinels"]) == {
        "static.hosp_los",
        "static.dialysis_free_days",
        "static.vent_free_days",
    }
    assert manifest["approved_numeric_missing_sentinels"][
        "static.hosp_los"
    ]["sentinel_counts"] == {"-1": 1}
    assert manifest["approved_numeric_missing_sentinels"][
        "static.dialysis_free_days"
    ]["sentinel_counts"] == {"-1": 1}
    assert manifest["approved_numeric_missing_sentinels"][
        "static.vent_free_days"
    ]["mapped_to_missing"] == 1
    assert (
        manifest["checks"]["approved_numeric_missing_sentinels_normalized"]
        is True
    )
    assert (
        manifest["checks"][
            "approved_hospital_specific_semantic_splits_applied"
        ]
        is True
    )
    assert manifest["checks"]["semantic_split_source_values_preserved"] is True
    split = manifest["approved_hospital_specific_semantic_splits"][
        "uk00_vt_per_ideal_bw_total"
    ]
    assert split["selected_hospital_ids"] == ["asic_UK00"]
    assert split["source_non_missing"] == 14
    assert split["selected_source_non_missing"] == 2
    assert split["nonselected_source_non_missing"] == 12
    assert split["base_output_non_missing"] == 12
    assert split["new_output_non_missing"] == 2
    assert split["base_output_non_missing_in_selected_hospitals"] == 0
    assert split["new_output_non_missing_outside_selected_hospitals"] == 0
    assert split["source_non_missing_values_preserved"] is True
    assert list(manifest["output"]["column_groups"]["dynamic"]) == [
        group.name for group in registry.dynamic_output_groups
    ]

    with pytest.raises(FileExistsError, match="--overwrite"):
        translate_and_write_pooled_data(config, reporter=None)


def test_translation_resolves_binary_alias_conflict_to_one(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    values = dynamic.column("LesebestaetigungTherapie_utc").to_pylist()
    values[0] = 1.0
    dynamic = dynamic.set_column(
        dynamic.schema.get_field_index("LesebestaetigungTherapie_utc"),
        "LesebestaetigungTherapie_utc",
        pa.array(values, type=pa.float32()),
    )
    pq.write_table(dynamic, dynamic_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    result = translate_and_write_pooled_data(config, reporter=None)

    translated = pq.read_table(result.dynamic_path)
    assert translated.column("therapy_read_confirmation_utc")[0].as_py() == 1.0
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    merge = manifest["approved_merges"]["therapy_confirmation_typo_variants"]
    assert merge["conflict_policy"] == "binary_or_fail_on_nonbinary"
    assert merge["conflict_rows"] == 1
    assert merge["resolved_conflict_rows"] == 1
    assert merge["invalid_binary_rows"] == {
        "LesebestaetigugnTherapie_utc": 0,
        "LesebestaetigungTherapie_utc": 0,
    }


def test_translation_maps_literal_bmi_nan_to_missing(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    static_path = pooled / "static.parquet"
    static = pq.read_table(static_path)
    values = static.column("BMI").to_pylist()
    values[0] = "nan"
    source_index = static.schema.get_field_index("BMI")
    static = static.set_column(
        source_index,
        "BMI",
        pa.array(values, type=static.schema.field(source_index).type),
    )
    pq.write_table(static, static_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    result = translate_and_write_pooled_data(config, reporter=None)

    translated = pq.read_table(result.static_path)
    assert translated.column("bmi_group")[0].as_py() is None
    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    mapping = manifest["approved_categorical_value_mappings"][
        "static.bmi_group"
    ]
    assert mapping["mapped_to_missing"] == 2
    assert mapping["normalized_source_counts"]["NAN"] == 1
    assert mapping["normalized_source_counts"]["X"] == 1


def test_translation_blocks_unmapped_categorical_value(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    static_path = pooled / "static.parquet"
    static = pq.read_table(static_path)
    values = static.column("clusterGeschlecht").to_pylist()
    values[0] = "X"
    source_index = static.schema.get_field_index("clusterGeschlecht")
    static = static.set_column(
        source_index,
        "clusterGeschlecht",
        pa.array(values, type=static.schema.field(source_index).type),
    )
    pq.write_table(static, static_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    with pytest.raises(ValueError, match="unmapped non-missing values"):
        translate_and_write_pooled_data(config, reporter=None)
    assert not config.output_translated_dir.exists()


def test_translation_blocks_stay_id_hospital_code_mismatch(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    static_path = pooled / "static.parquet"
    static = pq.read_table(static_path)
    values = static.column("hid").to_pylist()
    values[0] = 2.0
    source_index = static.schema.get_field_index("hid")
    static = static.set_column(
        source_index,
        "hid",
        pa.array(values, type=static.schema.field(source_index).type),
    )
    pq.write_table(static, static_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    with pytest.raises(ValueError, match="authoritative Pseudo-ID suffix"):
        translate_and_write_pooled_data(config, reporter=None)
    assert not config.output_translated_dir.exists()


def test_translation_blocks_nonbinary_therapy_confirmation(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    values = dynamic.column("LesebestaetigungTherapie_utc").to_pylist()
    values[0] = 2.0
    dynamic = dynamic.set_column(
        dynamic.schema.get_field_index("LesebestaetigungTherapie_utc"),
        "LesebestaetigungTherapie_utc",
        pa.array(values, type=pa.float32()),
    )
    pq.write_table(dynamic, dynamic_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    with pytest.raises(ValueError, match="non-binary values"):
        translate_and_write_pooled_data(config, reporter=None)
    assert not config.output_translated_dir.exists()


def test_translation_blocks_nonempty_approved_drop(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    values = dynamic.column("ARDS_Diagnose_App_duplicated_0").to_pylist()
    values[0] = 1.0
    dynamic = dynamic.set_column(
        dynamic.schema.get_field_index("ARDS_Diagnose_App_duplicated_0"),
        "ARDS_Diagnose_App_duplicated_0",
        pa.array(values, type=pa.float32()),
    )
    pq.write_table(dynamic, dynamic_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    with pytest.raises(ValueError, match="requires all missing"):
        translate_and_write_pooled_data(config, reporter=None)
    assert not config.output_translated_dir.exists()


def test_translation_blocks_alias_values_in_unapproved_hospital(
    tmp_path: Path,
) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    values = dynamic.column("CK").to_pylist()
    uk02_index = next(
        index
        for index, hospital in enumerate(dynamic.column("hid").to_pylist())
        if hospital == 2
    )
    values[uk02_index] = 123.0
    dynamic = dynamic.set_column(
        dynamic.schema.get_field_index("CK"),
        "CK",
        pa.array(values, type=pa.float32()),
    )
    pq.write_table(dynamic, dynamic_path)
    config = load_translation_config(_write_config(tmp_path, pooled))

    with pytest.raises(ValueError, match="disallowed hospitals"):
        translate_and_write_pooled_data(config, reporter=None)
    assert not config.output_translated_dir.exists()


def test_translation_accepts_nt_probnp_aliases_across_hospital_boundaries(
    tmp_path: Path,
) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    dynamic_path = pooled / "dynamic.parquet"
    dynamic = pq.read_table(dynamic_path)
    hospitals = dynamic.column("hid").to_pylist()
    uk00_index = hospitals.index(0)
    uk02_index = hospitals.index(2)

    standard_values = dynamic.column("NT-proBNP").to_pylist()
    underscore_values = dynamic.column("NT-pro_BNP").to_pylist()
    standard_values[uk02_index] = underscore_values[uk02_index]
    underscore_values[uk00_index] = standard_values[uk00_index]
    for source, values in (
        ("NT-proBNP", standard_values),
        ("NT-pro_BNP", underscore_values),
    ):
        source_index = dynamic.schema.get_field_index(source)
        dynamic = dynamic.set_column(
            source_index,
            source,
            pa.array(values, type=dynamic.schema.field(source_index).type),
        )
    pq.write_table(dynamic, dynamic_path)

    config = load_translation_config(_write_config(tmp_path, pooled))
    result = translate_and_write_pooled_data(config, reporter=None)
    translated = pq.read_table(result.dynamic_path)
    assert translated.column("ntprobnp")[uk00_index].as_py() == standard_values[
        uk00_index
    ]
    assert translated.column("ntprobnp")[uk02_index].as_py() == standard_values[
        uk02_index
    ]

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    merge = manifest["approved_merges"]["nt_probnp"]
    assert merge["overlap_rows"] == 2
    assert merge["conflict_rows"] == 0
    assert merge["disallowed_hospital_rows"] == {}


def test_translation_config_rejects_cross_context_output(tmp_path: Path) -> None:
    pooled, _ = _write_pooled_fixture(tmp_path)
    config_path = _write_config(tmp_path, pooled)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    output = tmp_path / "asic" / "data" / "production" / "translated"
    raw["paths"]["translated"] = str(output)
    config_path.write_text(
        yaml.safe_dump(raw, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="production"):
        load_translation_config(config_path)
