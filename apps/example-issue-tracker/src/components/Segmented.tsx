// A labeled segmented control — the Status / Priority pickers, shared by the create form (which
// updates local state) and the detail pane (which dispatches a mutation per selection).

export function Segmented<T extends string>({
  label,
  options,
  labels,
  value,
  onSelect,
}: {
  label: string;
  options: readonly T[];
  labels: Record<T, string>;
  value: T;
  onSelect: (value: T) => void;
}) {
  return (
    <div className="it-control-block">
      <span>{label}</span>
      <div className="it-segmented" role="group" aria-label={label}>
        {options.map((option) => (
          <button
            key={option}
            type="button"
            className={value === option ? "is-active" : ""}
            onClick={() => onSelect(option)}
          >
            {labels[option]}
          </button>
        ))}
      </div>
    </div>
  );
}
