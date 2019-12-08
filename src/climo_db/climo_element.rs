/// Elements we can query for climo data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClimoElement {
    HDW,
    BlowUpDt,
    BlowUpHeight,
    DCAPE,
}

impl ClimoElement {
    pub(crate) fn into_column_name(self) -> &'static str {
        use ClimoElement::*;

        match self {
            HDW => "hdw",
            BlowUpDt => "blow_up_dt",
            BlowUpHeight => "blow_up_meters",
            DCAPE => "dcape",
        }
    }
}
