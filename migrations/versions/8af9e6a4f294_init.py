"""init

Revision ID: 8af9e6a4f294
Revises:
Create Date: 2025-09-17 16:54:53.682667

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8af9e6a4f294"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("telegram_id", sa.BigInteger(), nullable=False, unique=True),
        sa.Column("full_name", sa.String(length=255), nullable=True),
        sa.Column("birthday", sa.Date(), nullable=True),
        sa.Column("wish", sa.Text(), nullable=True),
        sa.Column(
            "is_admin", sa.Boolean(), server_default=sa.text("FALSE"), nullable=False
        ),
        sa.Column("ward_id", sa.Integer(), nullable=True),
        sa.Column("giver_id", sa.Integer(), nullable=True),
        sa.Column(
            "registered_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # опционально: индекс по telegram_id (уникальность уже задана в колонке)
    # op.create_index("ix_users_telegram_id", "users", ["telegram_id"], unique=True)


def downgrade() -> None:
    # если создавали индекс в upgrade(), раскомментируй удаление индекса:
    # op.drop_index("ix_users_telegram_id", table_name="users")
    op.drop_table("users")
