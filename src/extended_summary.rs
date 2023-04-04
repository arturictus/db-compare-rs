use crate::Summary;

struct ExtendedSummary {
    pub table: String,
    pub updated: usize,
    pub deleted: usize,
    pub created: usize,
    pub updated_rows: Vec<u32>,
    pub updated_columns: HashMap<String, usize>,
}

impl ExtendedSummary {
    fn new(summary: &Summary) -> Self {
        Self {
            table: summary.table.clone(),
            updated: summary.updated,
            deleted: summary.deleted,
            created: summary.created,
            updated_rows: summary.updated_rows.clone(),
            updated_columns: HashMap::new(),
        }
    }

    fn extend(&mut self) {}
}
