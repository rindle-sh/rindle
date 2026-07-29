// The top bar: brand + the demo's "user" switcher — a typeahead dropdown over the real user cast
// (you can also type any handle to act as someone new). The user is just a display name persisted
// per browser (client.ts); a real app would show the signed-in identity here.

import { normalizeOwner } from "../../shared/app-def.ts";
import { Combobox } from "./Combobox.tsx";
import type { ComboItem } from "./Combobox.tsx";

export function TopBar({
  user,
  users,
  onSelect,
}: {
  user: string;
  users: ComboItem[];
  onSelect: (value: string) => void;
}) {
  return (
    <header className="it-topbar">
      <div className="it-brand" aria-label="Rindle Issues">
        <span className="it-mark" />
        <span>Rindle Issues</span>
      </div>
      <label className="it-user">
        <span>user</span>
        <Combobox
          value={user}
          items={users}
          placeholder="Pick a user"
          allowCustom
          formatCustom={normalizeOwner}
          onSelect={onSelect}
        />
      </label>
    </header>
  );
}
